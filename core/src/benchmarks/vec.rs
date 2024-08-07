extern crate test;

use test::Bencher;

#[bench]
fn _bench_concat_init(b: &mut Bencher) {
    b.iter(|| {
        let _x = vec![1i32; 100000];
        let _y = vec![2i32; 100000];
    });
}

#[bench]
fn bench_concat_append(b: &mut Bencher) {
    b.iter(|| {
        let mut x = vec![1i32; 100000];
        let mut y = vec![2i32; 100000];
        x.append(&mut y)
    });
}

#[bench]
fn bench_concat_extend(b: &mut Bencher) {
    b.iter(|| {
        let mut x = vec![1i32; 100000];
        let y = vec![2i32; 100000];
        x.extend(y)
    });
}

#[bench]
fn bench_concat_concat(b: &mut Bencher) {
    b.iter(|| {
        let x = vec![1i32; 100000];
        let y = vec![2i32; 100000];
        [x, y].concat()
    });
}

#[bench]
fn bench_concat_iter_chain(b: &mut Bencher) {
    b.iter(|| {
        let x = vec![1i32; 100000];
        let y = vec![2i32; 100000];
        x.into_iter().chain(y.into_iter())
    });
}

#[bench]
fn bench_concat_iter_chain_collect(b: &mut Bencher) {
    b.iter(|| {
        let x = vec![1i32; 100000];
        let y = vec![2i32; 100000];
        x.into_iter().chain(y.into_iter()).collect::<Vec<i32>>()
    });
}
