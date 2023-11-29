# mapreduce

Inspired by this paper: https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf, I implemented mapreduce in Rust. My implementation of Mapreduce supports coordinator <> worker communication via gRPC protocol. Supported functionality includes `grep` and finding the `word count` of a very large dataset (project gutenberg library). 

Transparency: I wrote all of `./proto`, `./worker`, `./coordinator`,  `client.rs` and `lib.rs.` The other folders were given to me as part of a course assignment from UC Berkeley's Operating Systems (CS162) course.
