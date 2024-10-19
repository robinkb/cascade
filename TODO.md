# TODO

## Common

- [ ] Repository name validation
  - [ ] Must conform to regex `[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*(\/[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*)*`
  - [ ] Should impose a reasonable limit: 255 characters for `hostname:port/name` is usual

## Pull

- [ ] Manifest reference validation: must be manifest digest or tag
  - [ ] Must as a tag be at most 128 characters in length
  - [ ] Must as a tag conform to regex `[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}`
- [ ] GET blobs should support `Range` request header in accordance to [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html#name-range-requests)
- [x] Blobs should be linked in repositories to ensure that only blobs that are referenced in a repository are pullable.
- [ ] Get manifests by tag

## Push

 - [x] Pushing a blob in chunks
   - [x] Get current upload status
   - [x] Calculate the digest server-side
     - [x] Write the test to verify that sending a wrong digest errors out
   - [x] Verify Content-Range in request
 - [ ] Mounting a blob from another repository
 - [ ] Push manifests with subject
 - [ ] Push tags

## Content Discovery

 - [ ] Listing tags
 - [ ] Listing referrers

## Content Management

- [ ] Deleting tags
- [x] Deleting manifests
- [ ] Deleting blobs
