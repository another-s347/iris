fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/helloworld/helloworld.proto")?;
    tonic_build::compile_protos("proto/n2n/n2n.proto")?;
    Ok(())
}