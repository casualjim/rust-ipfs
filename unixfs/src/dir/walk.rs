#![allow(unused, dead_code)]

use std::borrow::Cow;
use std::convert::TryFrom;
use std::fmt;
use crate::pb::{FlatUnixFs, PBLink, PBNode, ParsingFailed, UnixFsType};
use crate::file::{FileMetadata, FileReadFailed};
use crate::file::visit::{IdleFileVisit, FileVisit, Cache};
use crate::InvalidCidInLink;
use std::path::{Path, PathBuf};
use cid::Cid;

#[derive(Debug)]
pub struct Walker {
    current: InnerEntry,
    /// On the next call to `continue_walk` this will be the block, unless we have an ongoing file
    /// walk in which case we shortcircuit to continue it.
    next: Option<(Cid, String, usize)>,
    pending: Vec<(Cid, String, usize)>,
    // tried to recycle the names but that was consistently as fast and used more memory than just
    // cloning the strings
}

fn convert_link(
    depth: usize,
    nth: usize,
    link: PBLink<'_>,
) -> Result<(Cid, String, usize), InvalidCidInLink> {
    let hash = link.Hash.as_deref().unwrap_or_default();
    let cid = match Cid::try_from(hash) {
        Ok(cid) => cid,
        Err(e) => return Err(InvalidCidInLink::from((nth, link, e))),
    };
    let name = match link.Name {
        Some(Cow::Borrowed(s)) if !s.is_empty() => s.to_owned(),
        None | Some(Cow::Borrowed(_)) => todo!("link cannot be empty"),
        Some(Cow::Owned(s)) => s,
    };
    assert!(!name.contains('/'));
    Ok((cid, name, depth))
}

fn convert_sharded_link(
    depth: usize,
    nth: usize,
    link: PBLink<'_>,
) -> Result<(Cid, String, usize), InvalidCidInLink> {
    let hash = link.Hash.as_deref().unwrap_or_default();
    let cid = match Cid::try_from(hash) {
        Ok(cid) => cid,
        Err(e) => return Err(InvalidCidInLink::from((nth, link, e))),
    };
    let (depth, name) = match link.Name {
        Some(Cow::Borrowed(s)) if s.len() > 2 => (depth, s[2..].to_owned()),
        Some(Cow::Borrowed(s)) if s.len() == 2 => (depth - 1, String::from("")),
        None | Some(Cow::Borrowed(_)) => todo!("link cannot be empty"),
        Some(Cow::Owned(s)) => {
            if s.len() == 2 {
                (depth - 1, String::from(""))
            } else {
                assert!(s.len() > 2);
                (depth, s[2..].to_owned())
            }
        },
    };
    assert!(!name.contains('/'));
    Ok((cid, name, depth))
}

impl Walker {
    /// Starts a new walk on a block's `data`. Requires a `root_name` which can be empty, but
    /// conventionally with ipfs `/get` API is a string version of the root CID. It will be used as
    /// the filename for any top level file or symlink, or the directory name for the top level
    /// directory.
    ///
    /// Cache is an option, used to cache a datastructure between walking different files at the
    /// cost of more constantly higher memory usage. It can always be given as `&mut None` to
    /// effectively disable caching of the said datastructures.
    ///
    /// Returns on success the means to continue the walk or the final element and it's related
    /// data.
    pub fn start<'a>(data: &'a [u8], root_name: &str, cache: &mut Option<Cache>) -> Result<ContinuedWalk<'a>, Error> {
        let flat = FlatUnixFs::try_from(data)?;
        let metadata = FileMetadata::from(&flat.data);

        match flat.data.Type {
            UnixFsType::Directory => {
                let inner = InnerEntry::new_root_dir(metadata, root_name);

                let links = flat.links
                    .into_iter()
                    .enumerate()
                    // 2 == number of ancestors this link needs to have on the path, this is after
                    // some trial and error so not entirely sure why ... ancestors always include
                    // the empty root in our case.
                    .map(|(nth, link)| convert_link(2, nth, link));

                Self::walk_directory(links, inner)
            },
            UnixFsType::HAMTShard => {
                let inner = InnerEntry::new_root_bucket(metadata, root_name);

                // using depth == Path::ancestors().count() ... maybe not so good idea.
                let depth = if root_name.is_empty() {
                    1
                } else {
                    2
                };

                let links = flat.links
                    .into_iter()
                    .enumerate()
                    .map(move |(nth, link)| convert_sharded_link(depth, nth, link));

                Self::walk_directory(links, inner)
            },
            UnixFsType::Raw | UnixFsType::File => {
                let (bytes, file_size, metadata, step) = IdleFileVisit::default()
                    .start_from_parsed(flat, cache)?;

                let last = !step.is_some();
                let segment = FileSegment::first(bytes, last);
                let current = InnerEntry::new_root_file(metadata, root_name, step, file_size);

                let state = if !last {
                    State::Unfinished(Walker {
                        current,
                        next: None,
                        pending: Vec::new(),
                    })
                } else {
                    State::Last(current)
                };

                Ok(ContinuedWalk::File(segment, Item::from(state)))
            },
            UnixFsType::Metadata => todo!("metadata?"),
            UnixFsType::Symlink => {
                // symlinks are single block so

                let contents = match flat.data.Data {
                    Some(Cow::Borrowed(bytes)) if !bytes.is_empty() => bytes,
                    None | Some(Cow::Borrowed(_)) => &[][..],
                    _ => unreachable!("never used into_owned"),
                };

                let current = InnerEntry::new_root_symlink(metadata, root_name);

                Ok(ContinuedWalk::Symlink(contents, Item::from(State::Last(current))))
            },
        }
    }

    fn walk_directory<'a, I>(mut links: I, current: InnerEntry) -> Result<ContinuedWalk<'a>, Error>
        where I: Iterator<Item = Result<(Cid, String, usize), InvalidCidInLink>> + 'a,
    {
        let state = if let Some(next) = links.next() {
            let next = Some(next?);
            let pending = links.collect::<Result<Vec<_>, _>>()?;

            State::Unfinished(Walker { current, next, pending })
        } else {
            State::Last(current)
        };

        Ok(ContinuedWalk::Directory(Item::from(state)))
    }

    pub fn as_entry<'a>(&'a self) -> Entry<'a> {
        self.current.as_entry()
    }

    /// Returns the next cid to load and pass content of which to pass to `continue_walk`.
    pub fn pending_links(&self) -> (&Cid, impl Iterator<Item = &Cid>) {
        use InnerKind::*;
        // rev: because we'll pop any of the pending
        let cids = self.pending.iter()
            .map(|(cid, ..)| cid)
            .rev();

        match self.current.kind {
            File(_, Some(ref visit), _) => {
                let (first, rest) = visit.pending_links();
                let next = self.next.iter().map(|(cid, _, _)| cid);
                (first, Either::Left(rest.chain(next.chain(cids))))
            },
            _ => {
                let next = self.next.as_ref()
                    .expect("validated in start and continue_walk we have the next");
                (&next.0, Either::Right(cids))
            }
        }
    }

    pub fn continue_walk<'a>(mut self, bytes: &'a [u8], cache: &mut Option<Cache>) -> Result<ContinuedWalk<'a>, Error> {
        use InnerKind::*;

        match &mut self.current.kind {
            File(_, visit @ Some(_), _) => {
                let (bytes, step) = visit.take()
                    .unwrap()
                    .continue_walk(bytes, cache)?;

                let file_continues = step.is_some();
                *visit = step;

                let segment = FileSegment::later(bytes, !file_continues);

                let state = if file_continues || self.next.is_some() {
                    State::Unfinished(self)
                } else {
                    State::Last(self.current)
                };

                return Ok(ContinuedWalk::File(segment, Item::from(state)))
            },
            _ => {}
        }

        let flat = FlatUnixFs::try_from(bytes)?;
        let metadata = FileMetadata::from(&flat.data);

        match flat.data.Type {
            UnixFsType::Directory => {
                let (cid, name, depth) = self.next.expect("continued without next");
                self.current.as_directory(
                    cid,
                    &name,
                    depth,
                    metadata,
                );

                // depth + 1 because all entries below a directory are children of next, as in,
                // deeper
                let mut links = flat.links
                    .into_iter()
                    .enumerate()
                    .map(|(nth, link)| convert_link(depth + 1, nth, link))
                    .rev();

                // replacing this with try_fold takes as many lines as the R: Try<Ok = B> cannot be
                // deduced without specifying the Error

                let mut pending = {
                    let mut pending = self.pending;
                    for link in links {
                        pending.push(link?);
                    }
                    pending
                };

                let state = if let Some(next) = pending.pop() {
                    State::Unfinished(Self {
                        current: self.current,
                        next: Some(next),
                        pending,
                    })
                } else {
                    State::Last(self.current)
                };

                Ok(ContinuedWalk::Directory(Item::from(state)))
            },
            UnixFsType::HAMTShard => {
                // FIXME: the first hamtshard must have metadata!
                let (cid, name, depth) = self.next.expect("continued without next");

                if name.is_empty() {
                    // the name should be empty for all of the siblings
                    self.current.as_bucket(cid, &name, depth);
                } else {
                    // but it should be non-empty for the directories
                    self.current.as_bucket_root(cid, &name, depth, metadata);
                }

                // similar to directory the depth is +1 for nested entries, but the sibling buckets
                // are at depth
                let mut links = flat.links
                    .into_iter()
                    .enumerate()
                    .map(|(nth, link)| convert_sharded_link(depth + 1, nth, link))
                    .rev();

                let mut pending = {
                    let mut pending = self.pending;
                    for link in links {
                        pending.push(link?);
                    }
                    pending
                };

                let state = if let Some(next) = pending.pop() {
                    State::Unfinished(Self {
                        current: self.current,
                        next: Some(next),
                        pending,
                    })
                } else {
                    State::Last(self.current)
                };

                Ok(ContinuedWalk::Directory(Item::from(state)))

            },
            UnixFsType::Raw | UnixFsType::File => {
                let (bytes, file_size, metadata, step) = IdleFileVisit::default()
                    .start_from_parsed(flat, cache)?;

                let (cid, name, depth) = self.next.expect("continued without next");
                let file_continues = step.is_some();
                self.current.as_file(cid, &name, depth, metadata, step, file_size);

                let next = self.pending.pop();

                // FIXME: add test case for this being reversed and it's never the last
                let segment = FileSegment::first(bytes, !file_continues);

                let state = if file_continues || next.is_some() {
                    State::Unfinished(Self {
                        current: self.current,
                        next,
                        pending: self.pending,
                    })
                } else {
                    State::Last(self.current)
                };

                Ok(ContinuedWalk::File(segment, Item::from(state)))
            },
            UnixFsType::Metadata => todo!("metadata?"),
            UnixFsType::Symlink => {
                let contents = match flat.data.Data {
                    Some(Cow::Borrowed(bytes)) if !bytes.is_empty() => bytes,
                    None | Some(Cow::Borrowed(_)) => &[][..],
                    _ => unreachable!("never used into_owned"),
                };

                let (cid, name, depth) = self.next.expect("continued without next");
                self.current.as_symlink(cid, &name, depth, metadata);

                let state = if let Some(next) = self.pending.pop() {
                    State::Unfinished(Self {
                        current: self.current,
                        next: Some(next),
                        pending: self.pending,
                    })
                } else {
                    State::Last(self.current)
                };

                Ok(ContinuedWalk::Symlink(contents, Item::from(state)))
            },
        }
    }

    fn skip_current_file(mut self) -> Skipped {
        use InnerKind::*;
        match &mut self.current.kind {
            File(_, visit @ Some(_), _) => {
                visit.take();

                if self.next.is_some() {
                    Skipped(State::Unfinished(self))
                } else {
                    Skipped(State::Last(self.current))
                }
            },
            Bucket(_) => todo!("we could skip shards as well by ... maybe?"),
            ref x => todo!("how to skip {:?}", x),
        }
    }

    // TODO: we could easily split a 'static value for a directory or bucket, which would pop all
    // entries at a single level out to do some parallel walking, though the skipping could already
    // be used to do that... Maybe we could return the filevisit on Skipped to save user from
    // re-creating one? How to do the same for directories?
}

enum Either<A, B> {
    Left(A),
    Right(B),
}

impl<A, B> Iterator for Either<A, B>
    where A: Iterator,
          B: Iterator<Item = <A as Iterator>::Item>
{
    type Item = A::Item;

    fn next(&mut self) -> Option<Self::Item> {
        use Either::*;
        match self {
            Left(a) => a.next(),
            Right(b) => b.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        use Either::*;
        match self {
            Left(a) => a.size_hint(),
            Right(b) => b.size_hint(),
        }
    }
}

#[derive(Debug)]
struct InnerEntry {
    kind: InnerKind,
    path: PathBuf,
    metadata: FileMetadata,
    depth: usize,
}

impl From<InnerEntry> for FileMetadata {
    fn from(e: InnerEntry) -> Self {
        e.metadata
    }
}

#[derive(Debug)]
enum InnerKind {
    // This is necessarily at the root of the walk
    RootDirectory,
    // This is necessarily at the root of the walk
    BucketAtRoot,
    // This is the metadata containing bucket, for which we have a name
    RootBucket(Cid),
    // This is a sibling to a previous named metadata containing bucket
    Bucket(Cid),
    Directory(Cid),
    // FIXME: could simplify roots to optinal cid variants?
    File(Option<Cid>, Option<FileVisit>, u64),
    Symlink(Option<Cid>),
}

#[derive(Debug)]
pub enum Entry<'a> {
    RootDirectory(&'a Path, &'a FileMetadata),
    Bucket(&'a Cid, &'a Path),
    Directory(&'a Cid, &'a Path, &'a FileMetadata),
    // TODO: add remaining bytes or something here?
    File(Option<&'a Cid>, &'a Path, &'a FileMetadata, u64),
    Symlink(Option<&'a Cid>, &'a Path, &'a FileMetadata),
}

impl<'a> Entry<'a> {
    /// Returns the path for the latest entry. This is created from UTF8 string and as such always
    /// representable on all platforms.
    pub fn path(&self) -> &'a Path {
        use Entry::*;
        match self {
            RootDirectory(p, _)
            | Bucket(_, p)
            | Directory(_, p, _)
            | File(_, p, _, _)
            | Symlink(_, p, _) => p,
        }
    }

    /// Returns the metadata for the latest entry. It exists for initial directory entries, files,
    /// and symlinks but not continued HamtShards.
    pub fn metadata(&self) -> Option<&'a FileMetadata> {
        use Entry::*;
        match self {
            Bucket(_, _) => None,
            RootDirectory(_, m)
            | Directory(_, _, m)
            | File(_, _, m, _)
            | Symlink(_, _, m) => Some(m),
        }
    }

    /// Returns the total size of the file this entry represents, or none if not a file.
    pub fn total_file_size(&self) -> Option<u64> {
        use Entry::*;
        match self {
            File(_, _, _, sz) => Some(*sz),
            _ => None,
        }
    }

    pub fn cid(&self) -> Option<&Cid> {
        use Entry::*;
        match self {
            RootDirectory(_, _)
            | File(None, _, _, _)
            | Symlink(None, _, _) => None,
            Bucket(cid, _)
            | Directory(cid, _, _)
            | File(Some(cid), _, _, _)
            | Symlink(Some(cid), _, _) => Some(cid),
        }
    }
}

impl InnerEntry {
    fn new_root_dir(metadata: FileMetadata, name: &str) -> Self {
        let mut path = PathBuf::new();
        path.push(name);
        Self {
            kind: InnerKind::RootDirectory,
            path,
            metadata,
            depth: if name.is_empty() { 0 } else { 1 },
        }
    }

    fn new_root_bucket(metadata: FileMetadata, name: &str) -> Self {
        let mut path = PathBuf::new();
        path.push(name);
        Self {
            kind: InnerKind::BucketAtRoot,
            path,
            metadata,
            depth: if name.is_empty() { 0 } else { 1 },
        }
    }

    fn new_root_file(metadata: FileMetadata, name: &str, step: Option<FileVisit>, file_size: u64) -> Self {
        let mut path = PathBuf::new();
        path.push(name);
        Self {
            kind: InnerKind::File(None, step, file_size),
            path,
            metadata,
            depth: if name.is_empty() { 0 } else { 1 },
        }
    }

    fn new_root_symlink(metadata: FileMetadata, name: &str) -> Self {
        let mut path = PathBuf::new();
        path.push(name);
        Self {
            kind: InnerKind::Symlink(None),
            path,
            metadata,
            depth: if name.is_empty() { 0 } else { 1 },
        }
    }

    pub fn as_entry<'a>(&'a self) -> Entry<'a> {
        use InnerKind::*;
        match &self.kind {
            RootDirectory | BucketAtRoot => Entry::RootDirectory(&self.path, &self.metadata),
            RootBucket(cid) => Entry::Directory(cid, &self.path, &self.metadata),
            Bucket(cid) => Entry::Bucket(cid, &self.path),
            Directory(cid) => Entry::Directory(cid, &self.path, &self.metadata),
            File(cid, _, sz) => Entry::File(cid.as_ref(), &self.path, &self.metadata, *sz),
            Symlink(cid) => Entry::Symlink(cid.as_ref(), &self.path, &self.metadata),
        }
    }

    fn set_path(&mut self, name: &str, depth: usize) {

        while self.depth >= depth && self.depth > 0 {
            assert!(self.path.pop());
            self.depth -= 1;
        }

        self.path.push(name);
        self.depth = depth;
    }

    fn as_directory(
        &mut self,
        cid: Cid,
        name: &str,
        depth: usize,
        metadata: FileMetadata,
    ) {
        use InnerKind::*;
        match self.kind {
            RootDirectory
            | BucketAtRoot
            | Bucket(_)
            | RootBucket(_)
            | Directory(_)
            | File(_, None, _)
            | Symlink(_) => {
                self.kind = Directory(cid);
                self.set_path(name, depth);
                self.metadata = metadata;
            },
            ref x => todo!("dir after {:?}", x),
        }
    }

    fn as_bucket_root(
        &mut self,
        cid: Cid,
        name: &str,
        depth: usize,
        metadata: FileMetadata,
    ) {
        use InnerKind::*;
        match self.kind {
            RootDirectory
            | BucketAtRoot
            | Bucket(_)
            | RootBucket(_)
            | Directory(_)
            | File(_, None, _)
            | Symlink(_) => {
                self.kind = RootBucket(cid);
                self.set_path(name, depth);
                self.metadata = metadata;
            },
            ref x => todo!("root bucket after {:?}", x),
        }
    }

    fn as_bucket(
        &mut self,
        cid: Cid,
        name: &str,
        depth: usize
    ) {
        use InnerKind::*;
        match self.kind {
            BucketAtRoot => {
                assert_eq!(self.depth, depth, "{:?}", self.path);
            }
            RootBucket(_)
            | Bucket(_)
            | File(_, None, _)
            | Symlink(_) => {
                self.kind = Bucket(cid);

                if name.is_empty() {
                    // continuation bucket going bucket -> bucket
                    while self.depth > depth {
                        assert!(self.path.pop());
                        self.depth -= 1;
                    }
                } else {
                    self.set_path(name, depth);
                }

                assert_eq!(self.depth, depth, "{:?}", self.path);
            },
            ref x => todo!("bucket after {:?}", x),
        }
    }

    fn as_file(
        &mut self,
        cid: Cid,
        name: &str,
        depth: usize,
        metadata: FileMetadata,
        step: Option<FileVisit>,
        file_size: u64,
    ) {
        use InnerKind::*;
        match self.kind {
            RootDirectory
            | BucketAtRoot
            | RootBucket(_)
            | Bucket(_)
            | Directory(_)
            | File(_, None, _)
            | Symlink(_) => {
                self.kind = File(Some(cid), step, file_size);
                self.set_path(name, depth);
                self.metadata = metadata;
            },
            ref x => todo!("file from {:?}", x),
        }
    }

    fn as_symlink(
        &mut self,
        cid: Cid,
        name: &str,
        depth: usize,
        metadata: FileMetadata
    ) {
        use InnerKind::*;
        match self.kind {
            RootDirectory
            | BucketAtRoot
            | RootBucket(_)
            | Bucket(_)
            | Directory(_)
            | File(_, None, _)
            | Symlink(_) => {
                self.kind = Symlink(Some(cid));
                self.set_path(name, depth);
                self.metadata = metadata;
            },
            ref x => todo!("symlink from {:?}", x),
        }
    }
}

// Wrapper to hide the state
#[derive(Debug)]
pub struct Item {
    state: State,
}

impl From<State> for Item {
    fn from(state: State) -> Self {
        Item {
            state
        }
    }
}

impl Item {
    // TODO: add path(&self) -> &Path

    pub fn as_entry(&self) -> Entry<'_> {
        match &self.state {
            State::Unfinished(w) => w.as_entry(),
            State::Last(w) => w.as_entry(),
        }
    }

    pub fn into_inner(self) -> Option<Walker> {
        match self.state {
            State::Unfinished(w) => Some(w),
            _ => None,
        }
    }
}

#[derive(Debug)]
enum State {
    Unfinished(Walker),
    Last(InnerEntry),
}

#[derive(Debug)]
pub enum ContinuedWalk<'a> {
    File(FileSegment<'a>, Item),
    Directory(Item),
    Symlink(&'a [u8], Item),
}

impl ContinuedWalk<'_> {
    /// Returns the current entry describing Item, helpful when only listing the tree.
    pub fn into_inner(self) -> Item {
        use ContinuedWalk::*;

        match self {
            File(_, item) | Directory(item) | Symlink(_, item) => item,
        }
    }
}

#[derive(Debug)]
pub struct Skipped(State);

#[derive(Debug)]
pub struct FileSegment<'a> {
    bytes: &'a [u8],
    first_block: bool,
    last_block: bool,
}

impl<'a> FileSegment<'a> {
    fn first(bytes: &'a [u8], last_block: bool) -> Self {
        FileSegment {
            bytes,
            first_block: true,
            last_block,
        }
    }

    fn later(bytes: &'a [u8], last_block: bool) -> Self {
        FileSegment {
            bytes,
            first_block: false,
            last_block,
        }
    }

    /// Returns true if this is the first block of the file, false otherwise.
    ///
    /// Note: First block can also be the last.
    pub fn is_first(&self) -> bool {
        self.first_block
    }

    /// Returns true if this is the last block of the file, false otherwise.
    ///
    /// Note: Last block can also be the first.
    pub fn is_last(&self) -> bool {
        self.last_block
    }
}

impl AsRef<[u8]> for FileSegment<'_> {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Debug)]
pub enum Error {

}

impl From<ParsingFailed<'_>> for Error {
    fn from(e: ParsingFailed<'_>) -> Self {
        todo!()
    }
}

impl From<InvalidCidInLink> for Error {
    fn from(e: InvalidCidInLink) -> Self {
        todo!()
    }
}

impl From<FileReadFailed> for Error {
    fn from(e: FileReadFailed) -> Self {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use crate::file::tests::FakeBlockstore;

    #[test]
    fn walk_two_file_directory_empty() {
        two_file_directory_scenario("");
    }

    #[test]
    fn walk_two_file_directory_named() {
        two_file_directory_scenario("foo");
    }

    fn two_file_directory_scenario(root_name: &str) {
        println!("new two_file_directory_scenario");
        let mut counts = walk_everything(root_name, "QmPTotyhVnnfCu9R4qwR4cdhpi5ENaiP8ZJfdqsm8Dw2jB");

        let mut pb = PathBuf::new();
        pb.push(root_name);
        counts.checked_removal(&pb, 1);

        pb.push("QmVkvLsSEm2uJx1h5Fqukje8mMPYg393o5C2kMCkF2bBTA");
        counts.checked_removal(&pb, 1);

        pb.push("foobar.balanced");
        counts.checked_removal(&pb, 5);

        assert!(pb.pop());
        pb.push("foobar.trickle");
        counts.checked_removal(&pb, 5);

        assert!(counts.is_empty(), "{:#?}", counts);
    }

    #[test]
    fn sharded_dir_different_root_empty() {
        sharded_dir_scenario("");
    }

    #[test]
    fn sharded_dir_different_root_named() {
        sharded_dir_scenario("foo");
    }

    fn sharded_dir_scenario(root_name: &str) {
        use std::fmt::Write;

        // the hamt sharded directory is such that the root only has buckets so all of the actual files
        // are at second level buckets, each bucket should have 2 files. the actual files is in fact a single empty
        // file, linked from many names.

        let mut counts = walk_everything(root_name, "QmZbFPTnDBMWbQ6iBxQAhuhLz8Nu9XptYS96e7cuf5wvbk");
        let mut buf = PathBuf::from(root_name);

        counts.checked_removal(&buf, 9);

        let indices = [38, 48, 50, 58, 9, 33, 4, 34, 17, 37, 40, 16, 41, 3, 25, 49];
        let mut fmtbuf = String::new();

        for (index, i) in indices.iter().enumerate() {
            fmtbuf.clear();
            write!(fmtbuf, "long-named-file-{:03}", i).unwrap();

            if index > 0 {
                buf.pop();
            }
            buf.push(&fmtbuf);

            counts.checked_removal(&buf, 1);
        }

        assert!(counts.is_empty(), "{:#?}", counts);
    }

    #[test]
    fn top_level_single_block_file_empty() {
        single_block_top_level_file_scenario("");
    }

    #[test]
    fn top_level_single_block_file_named() {
        single_block_top_level_file_scenario("empty.txt");
    }

    fn single_block_top_level_file_scenario(root_name: &str) {
        let mut counts = walk_everything(root_name, "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH");
        let mut buf = PathBuf::from(root_name);
        counts.checked_removal(&buf, 1);
    }

    #[test]
    fn top_level_symlink_empty() {
        top_level_symlink_scenario("");
    }

    #[test]
    fn top_level_symlink_named() {
        top_level_symlink_scenario("this_links_to_foobar");
    }

    fn top_level_symlink_scenario(root_name: &str) {
        let mut counts = walk_everything(root_name, "QmNgQEdXVdLw79nH2bnxLMxnyWMaXrijfqMTiDVat3iyuz");
        let mut buf = PathBuf::from(root_name);
        counts.checked_removal(&buf, 1);
    }

    #[test]
    fn top_level_multiblock_file_empty() {
        top_level_multiblock_file_scenario("");
    }

    #[test]
    fn top_level_multiblock_file_named() {
        top_level_multiblock_file_scenario("foobar_and_newline.txt");
    }

    fn top_level_multiblock_file_scenario(root_name: &str) {
        let mut counts = walk_everything(root_name, "QmWfQ48ChJUj4vWKFsUDe4646xCBmXgdmNfhjz9T7crywd");
        let mut buf = PathBuf::from(root_name);
        counts.checked_removal(&buf, 5);
    }

    trait CountsExt {
        fn checked_removal(&mut self, key: &PathBuf, expected: usize);
    }

    impl CountsExt for HashMap<PathBuf, usize> {
        fn checked_removal(&mut self, key: &PathBuf, expected: usize) {
            use std::collections::hash_map::Entry::*;

            match self.entry(key.clone()) {
                Occupied(oe) => {
                    assert_eq!(oe.remove(), expected);
                },
                Vacant(_) => {
                    panic!("no such key {:?} (expected {}) in {:#?}", key, expected, self);
                }
            }
        }
    }

    fn walk_everything(root_name: &str, cid: &str) -> HashMap<PathBuf, usize> {
        let mut ret = HashMap::new();

        let blocks = FakeBlockstore::with_fixtures();
        let block = blocks.get_by_str(cid);
        let mut cache = None;

        let item = Walker::start(block, root_name, &mut cache).unwrap().into_inner();

        *ret.entry(PathBuf::from(item.as_entry().path())).or_insert(0) += 1;
        let mut visit = item.into_inner();

        while let Some(walker) = visit {
            let (next, _) = walker.pending_links();
            let block = blocks.get_by_cid(next);
            let item = walker.continue_walk(block, &mut cache).unwrap().into_inner();
            *ret.entry(PathBuf::from(item.as_entry().path())).or_insert(0) += 1;
            visit = item.into_inner();
        }

        ret
    }
}
