# Contributing guidelines

## Code formatting

We follow the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html). See
https://github.com/google/google-java-format for IDE plugins. The rules are not configurable.

The build will fail if the code is not formatted. To format all files from the command line, run:
 
```
mvn fmt:format -Dformat.validateOnly=false
```

Some aspects are not covered by the formatter:
* imports: please configure your IDE to follow the guide (no wildcard imports, normal imports 
  in ASCII sort order come first, followed by a blank line, followed by static imports in ASCII
  sort order).
* braces must be used with `if`, `else`, `for`, `do` and `while` statements, even when the body is
  empty or contains only a single statement.
* implementation comments: wrap them to respect the column limit of 100 characters.
* XML files: indent with two spaces and wrap to respect the column limit of 100 characters.


## Coding style -- production code

Do not use static imports. They make things harder to understand when you look at the code 
someplace where you don't have IDE support, like Github's code view.

Avoid abbreviations in class and variable names. A good rule of thumb is that you should only use
them if you would also do so verbally, for example "id" and "config" are probably reasonable.
Single-letter variables are permissible if the variable scope is only a few lines, or for commonly
understood cases (like `i` for a loop index).

Keep source files short. Short files are easy to understand and test. The average should probably 
be around 200-300 lines. 

### Javadoc

All types in "API" packages must be documented. For "internal" packages, documentation is optional,
but in no way discouraged: it's generally a good idea to have a class-level comment that explains
where the component fits in the architecture, and anything else that you feel is important.

You don't need to document every parameter or return type, or even every method. Don't document 
something if it is completely obvious, we don't want to end up with this:

```java
/**
 * Returns the name.
 * 
 * @return the name
 */
String getName();
```

On the other hand, there is often something useful to say about a method, so most should have at
least a one-line comment. Use common sense.

Driver users coding in their IDE should find the right documentation at the right time. Try to 
think of how they will come into contact with the class. For example, if a type is constructed with
a builder, each builder method should probably explain what the default is when you don't call it.

Avoid using too many links, they can make comments harder to read, especially in the IDE. Link to a
type the first time it's mentioned, then use a text description ("this registry"...) or an `@code`
block. Don't link to a class in its own documentation. Don't link to types that appear right below
in the documented item's signature.

```java
/**
* @return this {@link Builder} <-- completely unnecessary
*/
Builder withLimit(int limit) {
```


## Coding style -- test code

Static imports are permitted in a couple of places:
* AssertJ's `assertThat` / `fail`.
* Some Mockito methods, provided that you're already using a non-statically imported method at the
  beginning of the line. For example:
  ```java
  // any and eq are statically imported, it's pretty clear that they at least relate to Mockito
  Mockito.verify(intCodec).decodePrimitive(any(ByteBuffer.class), eq(ProtocolVersion.DEFAULT));
  ```

Test methods names use lower snake case, generally start with `should`, and clearly indicate the
purpose of the test, for example: `should_fail_if_key_already_exists`. If you have trouble coming 
up with a simple name, it might be a sign that your test does too much, and should be split.

We use AssertJ (`assertThat`) for assertions. Don't use TestNG's assertions (`assertEquals`,
`assertNull`, etc).

Don't try to generify at all cost: a bit of duplication is acceptable, if that helps keep the tests
simple to understand (a newcomer should be able to understand how to fix a failing test without
having to read too much code).

## License headers

The build will fail if some license headers are missing. To update all files from the command line,
run:

```
mvn license:format
```

## Pre-commit hook (highly recommended)
 
Ensure `pre-commit.sh` is executable, then run:

```
ln -s ../../pre-commit.sh .git/hooks/pre-commit
```

This will only allow commits if the tests pass. It is also a good reminder to keep the test suite
short.

Note: the tests run on the current state of the working directory. I tried to add a `git stash` in
the script to only test what's actually being committed, but I couldn't get it to run reliably
(it's still in there but commented). Keep this in mind when you commit, and don't forget to re-add
the changes if the first attempt failed and you fixed the tests.
