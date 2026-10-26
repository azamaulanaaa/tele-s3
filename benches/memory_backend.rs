use criterion::{Criterion, criterion_group, criterion_main};
use futures::executor::block_on;
use futures::io::Cursor;
use tele_s3::backend::{Backend, Memory, new_publish_id};

fn bench_memory_write(c: &mut Criterion) {
    c.bench_function("memory_write_64b", |b| {
        b.iter(|| {
            block_on(async {
                let backend = Memory::<16, 64>::default();
                let id = new_publish_id();
                let body = Box::pin(Cursor::new(vec![0u8; 64]));
                let _ = backend.write(id, 64, body).await.expect("write succeeds");
            })
        })
    });
}

fn bench_memory_read(c: &mut Criterion) {
    c.bench_function("memory_read_64b", |b| {
        b.iter_batched(
            || {
                block_on(async {
                    let backend = Memory::<16, 64>::default();
                    let id = new_publish_id();
                    let body = Box::pin(Cursor::new(vec![0u8; 64]));
                    let key = backend.write(id, 64, body).await.expect("write succeeds");
                    (backend, key)
                })
            },
            |(backend, key)| {
                block_on(async {
                    let mut reader = match backend.read(key, 0, None).await.expect("read succeeds") {
                        tele_s3::backend::ReadOutcome::Present(r) => r,
                        _ => panic!("expected present"),
                    };
                    let mut out = Vec::new();
                    let _ = futures::AsyncReadExt::read_to_end(&mut *reader, &mut out)
                        .await
                        .expect("read completes");
                    std::hint::black_box(out);
                })
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group!(memory_benches, bench_memory_write, bench_memory_read);
criterion_main!(memory_benches);