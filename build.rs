fn main() {
    let proto = "proto/santaizi.proto";
    println!("cargo:rerun-if-changed={proto}");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[proto], &["proto"])
        .expect("compile santaizi.proto");
}
