pub mod demo {
    pub mod common {
        include!(concat!(env!("OUT_DIR"), "/demo.common.rs"));
    }

    pub mod assignment {
        include!(concat!(env!("OUT_DIR"), "/demo.assignment.rs"));
    }

    pub mod example1 {
        include!(concat!(env!("OUT_DIR"), "/demo.example1.rs"));
    }
}