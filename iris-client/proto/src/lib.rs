pub mod hello_world {
    tonic::include_proto!("helloworld");
}

pub mod n2n {
    tonic::include_proto!("n2n");
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
