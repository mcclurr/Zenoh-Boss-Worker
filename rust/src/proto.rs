pub mod demo {
    pub mod common {
        include!(concat!(env!("OUT_DIR"), "/demo.common.rs"));
    }

    pub mod example1 {
        include!(concat!(env!("OUT_DIR"), "/demo.example1.rs"));
    }
}