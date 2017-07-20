## Frequently asked questions

### I'm modifying a statement and the changes get ignored, why?

In driver 4, statement classes are immutable. All mutating methods return a new instance, so make
sure you don't accidentally ignore their result:

```java
BoundStatement boundSelect = preparedSelect.bind();

// This doesn't work: setInt doesn't modify boundSelect in place:
boundSelect.setInt("k", key);
session.execute(boundSelect);

// Instead, do this:
boundSelect = boundSelect.setInt("k", key);
```