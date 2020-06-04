pub mod hello_world {
    tonic::include_proto!("helloworld");
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
