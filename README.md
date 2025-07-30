# Chfs

## Introduction

这是一个类似于GFS的inode分布式文件系统，在此基础上实现了raft协议和map-reduce任务。你可以查看文档获得具体的信息。

1. [Basic Filesystem](docs/fs/fs.md)
2. [Distributed FileSystem](docs/dfs/dfs.md)
3. [Raft](docs/raft/raft.md)
4. [MapReduce With Distributed File System](docs/mr/mr.md)

<a id="run-demo"></a>
## Run Demo

We use [the libfuse userspace library](http://libfuse.github.io/doxygen/index.html) provided by FUSE (Filesystem in Userspace) to implement the adaptor layer. You can refer to `daemons/distributed/main.cc` for the detailed implementation.

We use `docker compose` to set up the environment, so you might need to install `docker compose` plugin first. You can refer to [Overview of installing Docker Compose](https://docs.docker.com/compose/install/) for more details.

To launch the environment, start a shell and execute the following commands under the `scripts/lab2` directory:

```bash
# pull cse-lab2-base image
docker pull registry.cn-shenzhen.aliyuncs.com/cse-lab/cse-lab2-base
# rename the image
docker tag registry.cn-shenzhen.aliyuncs.com/cse-lab/cse-lab2-base cse-lab2-base
# set up the environment
docker compose up
```

After this, all containers are launched and initiated. You can use `docker ps` to check the status of containers. Then **open another shell** and execute the following command to enter the container that mounts the filesystem:

```bash
docker exec -it lab2-fs_client-1 bash
```

The mount point is at `/tmp/mnt` in the `lab2-fs_client-1` container. The filesystem you have implemented is mounted to this directory, which means that every filesystem request inside this directory will be fulfilled by the filesystem you have implemented.

### Run Demo (Not Included in Grading)

- You can create a new directory:

  ```bash
  mkdir my_dir 
  ```

- Then you can create a new file inside the newly created directory:

  ```bash
  touch my_dir/a.txt 
  ```

- Check whether the file is created successfully:

  ```bash
  ls my_dir 
  ```

- After that, you can write something into this file:

  ```bash
  echo "foo" >> my_dir/a.txt
  echo "bar" >> my_dir/a.txt
  ```

- Read the file, then you will see the contents you have just written:

  ```bash
  cat my_dir/a.txt 
  ```

- Then if you do not need this file anymore, delete it:

  ```bash
  rm my_dir/a.txt 
  ```
