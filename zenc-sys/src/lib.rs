#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_zimd_max() {
        let vec = [1.0, 2.0, 3.0];
        let max = unsafe { zimd_max_f64(vec.as_ptr(), vec.len()) };
        assert_eq!(max, 3.0);
    }
}
