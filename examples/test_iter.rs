use std::default;
use std::slice::{Iter, IterMut};
fn main() {
    let v = vec![1, 2, 3];

    let v1_iter = v.iter();

    for val in v1_iter {
        println!("Got: {}", val);
    }

    let mut v2_iter = v.iter();
    let mut v3_iter = v.iter();
    let mut v4_iter = v.iter();

    println!("{}", v2_iter.next().unwrap());
    println!("{}", v3_iter.next().unwrap());
    println!("{}", v4_iter.next().unwrap());
    println!("{}", v2_iter.next().unwrap());
    println!("{:?}", v2_iter.next());
    println!("{:?}", v2_iter.next());
    println!("{:?}", v2_iter.next());

    let mut v5_iter : Iter<i32 >= Default::default();
    println!("{:#?}", v5_iter.next());
}