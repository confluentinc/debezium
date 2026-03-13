# Resolution Detail Files

This directory contains detailed resolution files for every RESOLVED (conflicting) cherry-pick commit.

## Complexity Levels
- **TRIVIAL:** Mechanical - accept deletion, keep both sides, single-line fix
- **MODERATE:** Multiple conflict regions with clear patterns, net-change merges
- **COMPLEX:** Interleaved features, superseded code detection, multi-method restructuring

## File Naming
- `commit-<progress#>-<source-sha-short>.md`
