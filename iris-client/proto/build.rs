fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = prost_build::Config::new();
    config.bytes(&["."]);
    tonic_build::configure()
        .compile_with_config(config, &[
            "proto/helloworld/helloworld.proto",
            "proto/n2n/n2n.proto"
        ], &[
            "proto/helloworld",
            "proto/n2n"
        ])?;
    Ok(())
}