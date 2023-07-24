Process for porting 1.8 updates into 1.9

For admins only:

1. cherry-pick the merged PR commits from develop into develop-1.9
2. Regenerate constraints.txt if constraints.in has been touched and commit.
3. force-push to develop-1.9 (before 1.9.0 release only)

After 1.9.0 release, go through a full PR process.
