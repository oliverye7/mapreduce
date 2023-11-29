# mapreduce

Transparency: I wrote the entirety of `./proto`, `./src/worker`, `./src/coordinator`,  `./src/client.rs` and `./src/lib.rs` from scratch. The other folders were given to me as part of a course assignment from UC Berkeley's Operating Systems (CS162) course, which I took in Spring 2023. I haven't edited the code from this directory since then, so there are a couple comments to myself (in `./coordinator`). 

I'm pretty proud of this repository since implementing MapReduce was pretty nontrivial. The code would be a lot cleaner for a production level code, but hopefully it demonstrates a baseline level of code cleanliness that I tend to write my code with; my `gRPC_image_rotation_service` repository demonstrates significantly more code clarity.

Inspired by this paper: https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf, I implemented mapreduce in Rust. My implementation of Mapreduce supports coordinator <> worker communication via gRPC protocol. Supported functionality includes `grep` and finding the `word count` of a very large dataset (project gutenberg library). 

The project supportsa fault tolerant, scalable task distribution coordinator to worker processes. The coordinator system has graceful failure handling by checking on worker heartbeats and redistributing tasks from workers which have died. 
