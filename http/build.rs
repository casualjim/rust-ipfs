fn main() {
    // vergen will generate build flags which will allow direct usage of env!(...) macro to find
    // VERGEN_SHA_SHORT and others: https://docs.rs/vergen/5.1.15/vergen/
    //
    // the rebuild on each commit can be turned off:
    // https://docs.rs/vergen/3.0.4/vergen/struct.ConstantsFlags.html#associatedconstant.REBUILD_ON_HEAD_CHANGE
    vergen::vergen(vergen::Config::default()).expect("Unable to generate the cargo keys!");

    prost_build::compile_protos(&["src/keys.proto"], &["src"]).unwrap();
}
