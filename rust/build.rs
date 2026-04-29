fn main() {
    let mut config = prost_build::Config::new();
    config.compile_well_known_types();

    config
        .compile_protos(
            &[
                "../protos/common/common.proto",
                "../protos/common/metadata.proto",
                "../protos/example1/job.proto",
                "../protos/example1/result.proto",
                "../protos/example1/batch.proto",
            ],
            &["../protos"],
        )
        .expect("failed to compile protos");
}