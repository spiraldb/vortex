// We need to define for each array the entire set of compute operations it may need to implement.
// This feels kinda shitty.

// The alternative is that we define an efficient ArrayAccessor implementation for each of them,
// and delegate to that most of the time. Times when we can do better than canonicalizing:
//
//  (1) StringView, where you can do a straightline scan over the prefixes to avoid extra derefs
//  (2) RunEnd, where
