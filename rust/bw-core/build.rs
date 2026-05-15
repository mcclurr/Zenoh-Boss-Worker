fn main() {
    let mut config = prost_build::Config::new();
    config.compile_well_known_types();

    config
        .compile_protos(
            &[
                "../../proto/common/common.proto",
                "../../proto/common/metadata.proto",
                "../../proto/assignment/assignment.proto",
                "../../proto/chores/chores.proto",
                "../../proto/example1/job.proto",
                "../../proto/example1/input_pair.proto",
                "../../proto/example1/result.proto",
                "../../proto/example1/batch.proto",
            ],
            &["../../proto"],
        )
        .expect("failed to compile protos");
}