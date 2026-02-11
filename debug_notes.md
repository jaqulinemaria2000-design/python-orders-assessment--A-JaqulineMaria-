# Debug Notes
 
This file documents how I identified and fixed the two buggy functions.
 
## Bug 1 — `unsafe_bucketize`
 
**Symptom / Repro**
 
Calling `unsafe_bucketize` twice without passing `bucket` reused the same list:
 
```python
unsafe_bucketize([1, 2, 3, 4])   # -> [2, 4]
unsafe_bucketize([6])            # Expected: [6]; Actual (before fix): [2, 4, 6]
 
Root Cause
The function used a mutable default argument (bucket=[]). In Python, default arguments are evaluated once at function definition time, so the same list was shared across calls.
Fix
Change the signature to use None as the default and create a new list when needed:
 
def unsafe_bucketize(values: List[int], bucket: Optional[List[int]] = None) -> List[int]:
    if bucket is None:
        bucket = []
    for v in values:
        if v % 2 == 0:
            bucket.append(v)
    return bucket
 
This ensures each call (unless an explicit bucket is supplied) operates on a fresh list, giving deterministic and isolated results.
 
Bug 2 - merge_intervals_bad
 
On input [(1,3), (2,4), (6,7), (7,9)], the original function:
Mutated the caller’s list via intervals.sort().
Stored intervals as lists ([a, b]) and returned these, risking aliasing/mutation.
Used a strict comparison a < last_end which failed to merge adjacent intervals (e.g., (6,7) and (7,9)).
Overwrote the end with b even if b < last_end, potentially shrinking intervals. 
 
Fixes
No aliasing / caller surprise: Work on a sorted copy:
sorted_intervals = sorted(intervals, key=lambda ab: (ab[0], ab[1]))
 
Correct merge condition: Merge when a <= cur_end (include adjacency).
Correct end update: Use cur_end = max(cur_end, b).
Immutable return: Append tuples to merged to avoid shared references.
 
Final Implementation:
def merge_intervals_bad(intervals: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    if not intervals:
        return []
    sorted_intervals = sorted(intervals, key=lambda ab: (ab[0], ab[1]))
    merged: List[Tuple[int, int]] = []
    cur_start, cur_end = sorted_intervals[0]
    for a, b in sorted_intervals[1:]:
        if a <= cur_end:
            cur_end = max(cur_end, b)
        else:
            merged.append((cur_start, cur_end))
            cur_start, cur_end = a, b
    merged.append((cur_start, cur_end))
    return merged
 
Result
For [(1,3), (2,4), (6,7), (7,9)] → [(1,4), (6,9)], which is correct and deterministic.