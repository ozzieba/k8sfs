# K8s FS

This exposes your k8s cluster as a fuse filesystem. You should be able to list arbitrary resources, and probably even update them. (unless I just broke it)

It's a two-hour PoC hack with a lot of help from Claude 3, nowhere near production-ready or even safe to use. Please someone do this right.

needs pyyaml, fusepy, kubernetes to be installed

runs with ./k8sfs.py $DIR to mount an fs there
