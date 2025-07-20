# Tunqi

An Ergonomic ORM for Python.

- [Installation](#installation)
- [Quickstart](#quickstart)
- [Overview](#overview)
  - [Creating a Database](#creating-a-database)
  - [Creating, Updating and Deleting Models](#creating-updating-and-deleting-models)
  - [Selecting Models](#selecting-models)
  - [Foreign Keys](#foreign-keys)
  - [Backreferences and Many-to-Many Relations](#backreferences-and-many-to-many-relations)
  - [Advanced Queries](#advanced-queries)
  - [Database Management](#database-management)
  - [Migrations](#migrations)
  - [Model Management](#model-management)
    - [Lifecycle Methods](#lifecycle-methods)
    - [Fixed Queries](#fixed-queries)
    - [Model Subtrees](#model-subtrees)
    - [Deduplication](#deduplication)
    - [Customizing Table Names and Plural Nouns](#customizing-table-names-and-plural-nouns)
  - [Unique Constraints](#unique-constraints)
  - [Indexing](#indexing)
  - [JSON Support](#json-support)
  - [Upserting](#upserting)
  - [Distinct Counts](#distinct-counts)
  - [Aggregations](#aggregations)
  - [Functions](#functions)
  - [Column References](#column-references)
  - [Extending The Query Language](#extending-the-query-language)
  - [Extending the Type System](#extending-the-type-system)
  - [Escaping into SQL](#escaping-into-sql)
  - [Using the Database Directly](#using-the-database-directly)
  - [Auditing](#auditing)

## Installation

From PyPI:

```sh
$ pip install tunqi
...
```

From source:

```
$ git clone git@github.com:dan-gittik/tunqi.git
$ cd tunqi/
$ poetry install
...
```

And depending on which SQL dialects you work with:

- `pip install tunqi[sqlite]`;
- `pip install tunqi[postgresql]`;
- `pip install tunqi[mysql]`;
- or `pip install tunqi[all]`.

For full debug capabilities, run `pip install tunqi[debug]`

To test and develop the project, run `poetry install --with dev`.

## Overview

### Creating a Database

Tunqi provides identical synchronous and asynchronous APIs. It's async-first, so `from tunqi import ...` exposes the
asynchronous version, which we'll use for the rest of this tutorial; to get equivalent sycnrhonous code, swap it for
`from tunqi.sync import ...` and drop the `await`s.

The first thing we need is to set a default database. We'll see how to work with multiple or temporary databases later;
for now:

```pycon
>>> from tunqi import Database
>>> db = Database("sqlite:///:memory:", default=True)
```

I'm using an in-memory SQLite database, but we can similarly provide a URL starting with `postgresql://` for `mysql://`.
There's no need to specify drivers (like `aiosqlite` or `psycopg2`), as those are applied automatically, depending on
whether our code is sync or async.

### Creating, Updating and Deleting Models

Tunqi works with standard Pydantic models, but to make them part of the ORM, we have to subclass its `Model` rather than
Pydantic's `BaseModel`:

```pycon
>>> from tunqi import Model

>>> class User(Model):
...     name: str
```

Next, let's make sure the corresponding tables exist. We'll see how to manage separate schemas and handle migrations
later; for now:

```pycon
>>> await Model.create_tables()
```

All of Pydantic's functionality remains intact – only a special `pk` field is added automatically, which holds the
model's **primary key**. Initially it's `None`:

```pycon
>>> user = User(name="alice")
>>> user.name
'alice'
>>> user.pk
None
```

Which shows as `?` in the object's representation, indicating that it's unsaved:

```pycon
>>> user
User(?, name="alice")
```

When we save the record to the database, however, an auto-incrementing integer is assigned instead:

```pycon
>>> await user.save()
User(1, name="alice")
>>> user.pk
1
```

Once it's saved, we can fetch that same record by its primary key:

```
>>> user2 = await User.get(1)
>>> user2
User(1, name="alice")
>>> user2 == user
True
```

To update the record, we simply change the object and save it again:

```
>>> user.name = "bob"
>>> await user.save()
User(1, name="bob")
```

The fact that its `pk` is set tells the ORM it should issue an `UPDATE` statement rather than an `INSERT`, like it did
when the `pk` was `None`. Such updates are issued only when actual changes are detected; if we were to run this again:

```
>>> await user.save()
User(1, name="bob")
```

Nothing would reach the database, because the record hasn't actually changed. To see whether and how an object differs
from its saved state, we can do:

```
>>> user.changed()
{}
```

If we were to modify the object, we'd see field names mapped to tuples with their old and new values. We can do that by
assigning fields explicitly, like we did before, or by calling the `set` method, which lets us assign multiple fields at
once (as well as do so from a dictionary, e.g. `**fields`):

```
>>> user.set(name="charlie")
>>> user.name
'charlie'
>>> user.changed()
{'name': ('bob', 'charlie')}
```

If we were to save the object at this point, those changes would be committed and no longer tracked, since they'd now
match the saved state; but we can also discard them, bringing the object back to its persisted version:

```pycon
>>> user.reset()
>>> user.name
'bob'
>>> user.changed()
{}
```

Note that the fact that we updated one object doesn't reflect on the other, which still holds the state as it was when
that object was fetched:

```pycon
>>> user2.name
'alice'
```

Sometimes, this makes sense: we might not want objects to change under our feet just because they were modified in a
different task or thread. Other times, we want to automatically synchronize the same objects across the system, which
we'll cover when we talk about deduplication. For now, we can update the stale state by manually refreshing it:

```pycon
>>> await user2.refresh()
User(1, name='bob')
>>> user2.name
'bob'
```

Finally, we can delete a record from the database, which resets its `pk` to `None`:

```pycon
>>> await user.delete()
User(?, name='bob')
>>> user.pk
None
```

At which point, refreshing a stale version of it is no longer viable:

```pycon
>>> await user2.refresh()
DoesNotExistError: user with pk == 1 doesn't exist
```

One final note: as you might have noticed, `save`, `delete` and `refresh` return the user object, which is why its
representation appears in the interpreter after each call; you can safely ignore it, as in:

```python
await user.save()
```

But it can be useful for chaining, like when creating a user object and saving it in one line:

```python
user = await User(name="alice").save()
```

### Selecting Models

Naturally, there are many more things we can do other than save and delete objects. First, we can check if some records
exist:

```pycon
>>> await User.exists()
False
```

Or count how many of them do:

```pycon
>>> await User.count()
0
```

Not very exciting, admittedly, since our database is currently empty. To add a few records:

```pycon
>>> alice = User(name="alice")
>>> john = User(name="john")
>>> jane = User(name="jane")
```

We can save them one by one, like we did before, or use a **batch operation** that creates all of them at once:

```pycon
>>> await User.create(alice, john, jane)
[1, 2, 3]
>>> alice.pk
1
>>> john.pk
2
>>> jane.pk
3
```

And now:

```pycon
>>> await User.exists()
True
>>> await User.count()
3
```

To retrieve them, we can fetch each one separately – or, again, do so in one swing:

```pycon
>>> await User.all()
[User(1, name='alice'), User(2, name='john'), User(3, name='jane')]
```

So far, we've seen a way to reference a particular record via its `pk`, like in `get()`, which also works for `exists`:

```pycon
>>> await User.exists(1)
True
>>> await User.exists(4)
False
```

Or reference *all* records, like in `exists()` (with no arguments), `count()` and `all()`. But of course, such
operations are intended to be calibrated with *queries* – selecting some records and not others. The simplest query is
the direct comparison, equating a field to a value:

```pycon
>>> await User.exists(name="alice")
True
>>> await User.exists(name="bob")
False
```

More complex logics are expressed by combining the field names with additional operators, chained with double
underscores like so:

```pycon
>>> await User.count(name__startswith="j") # john and jane
2
>>> await User.all(name__endswith="e")
[User(1, name='alice'), User(3, name='jane')]
```

Those operators include:

- `<field>__ne=`: field does *not* equal;
- `<field>__gt=`: field is greater than;
- `<field>__ge=`: field is greater than or equal to;
- `<field>__lt=`: field is less than;
- `<field>__le=`: field is less than or equal to;
- `<field>__is=`: field is (identity; usually used with `None`, resulting in `field IS NULL`);
- `<field>__is_not=`: field is *not*;
- `<field>__in=`: field is one of several values;
- `<field>__not_in=`: field is *not* one of several values;
- `<field>__contains=`: field contains a substring (if it's a string) or a value (if it's a list);
- `<field>__has=`: field has a certain key (assuming it's a dictionary);
- `<field>__startswith=`: field start with a certain prefix (assuming it's a string);
- `<field>__endswith=`: field ends with a certain suffix (assuming it's a string);
- `<field>__like=`: field adheres to a certain SQL pattern (`%like this%`, assuming it's a string);
- `<field>__not_like=`: field does *not* adhere to a certain SQL pattern (assuming it's a string);
- `<field>__matches=`: field adheres to a certain regular expression (`.*like this.*`, assuming it's a string);

This syntax can also be used to traverse JSONs, join tables and invoke functions, and you can extend it to fit your own
custom needs – but more on all of that later.

Since this query language lets us articulate conditions that capture multiple records at once, it's particularly useful
for batch operations. We've already seen `create()`, but we also have `update()` and `delete_all()`; and just like
`create()` accepts multiple objects, so can these work on several instances at once:

```pycon
>>> await User.update(john, jane)(name="user") # Rename both john and jane to "user"...
2
>>> await User.delete_all(john, jane) # ... and delete both.
2
```

In fact, it can also work on PKs:

```pycon
>>> await User.create(john, jane) # Restore john and jane...
[2, 3]
>>> await User.update(2, 3)(name="user") # ... and do the same again.
2
>>> await User.delete_all(2, 3)
2
```

But more importantly, it can work with queries, which is why `update()` has this weird double invocation form: it makes
sure the *query* keywords (in the first invocation) are separate from the *update* keywords (in the second):

```pycon
>>> await User.create(john, jane)
>>> await User.update(name__startswith="j")(name="user") # Rename records whose name starts with "j" to "user"...
2
>>> await User.delete_all(name="user") # ... and delete them.
2
```

This also explains `update()`'s and `delete_all()`'s return value: they report back how many records were affected by
the operation, which is somewhat redundant when the objects or PKs are provided explicitly, but can be valuable
information when we specify a qualitative expression and want to know how many records actually fit its condition.

### Foreign Keys

One of SQL's key features is the ability to have one table reference another via **foreign keys**, also known as
many-to-one relations. Tunqi solves this in a pretty elegant way, if I might say so myself; it wasn't easy, making
Pydantic play nice with annotated descriptors, but everything you might need is achieved, and types are fully respected,
with the following minimal syntax:

```pycon
>>> from tunqi import FK

>>> class Post(Model):
...     author: FK[User]
...     title: str

>>> await Model.create_tables()
```

Objects with a foreign key can be created either with an actual object:

```pycon
>>> alice = await User(name="alice").save()
>>> post = Post(author=alice, title="Hello, world!")
```

Or with a PK:

```pycon
>>> alice.pk
1
>>> post = Post(author=1, title="Hello, world!")
```

If dumped (e.g. when serialized to JSON), however, we'll always get the PK:

```pycon
>>> post.model_dump()
{'pk': None, 'title': 'Hello, world!', 'author': 1}
```

Which can also be retrieved (and assigned) via the foreign key's `pk` property:

```pycon
>>> post.author.pk
1
```

The reasoning behind this is that sometimes, we start without the referenced object on hand; suppose we save our post
and then fetch it elsewhere:

```pycon
>>> await post.save()
Post(1, title='Hello, world!', author=1)

>>> post2 = await Post.get(1)
>>> post2
Post(1, title='Hello, world!', author=1)
```

As a record, `post2` knows that it references user #1, but it doesn't yet know what this user actually is; to get its
name, for example, we'd have to "dereference" it, so to speak, joining the tables to match this PK to its corresponding
fields. This is done with:

```pycon
>>> await post.author.get()
User(1, name='alice')
```

In which case, the dereferenced user object is cached, so subsequent `get()`s don't have to go to the database for it
(unless, that is, we want a fresh copy, in which case we pass in `fetch=True`). Similarly, to assign (or re-assign) a
foreign key, we do:

```pycon
>>> bob = await User(name="bob").save()
>>> await post.author.set(bob)
```

And note that foreign keys will protect themselves if we attempt to assign an object from the wrong model, or one that
hasn't been saved (and thus, can't be referenced) yet:

```pycon
>>> await post.author.set(post)
ValueError: can't set foreign key Post(1).author -> User to Post(1, title='Hello, world!', author=1) (expected User)
>>> charlie = User(name="charlie")
>>> await post.author.set(charlie)
ValueError: can't set foreign key Post(1).author -> User to the unsaved User(?, name='charlie')
```

Also note that foreign keys are required by default: we won't be able to save a post without an author, nor set it (or
its PK) to `None`; in fact, if the author gets deleted, it cascades to remove all the posts associated with it. When we
do want an optional foreign key, we use:

```pycon
>>> from tunqi import OptionalFK

>>> class Post:
...     author: OptionalFK[User]
...     title: str
```

In which case posts can be saved without an author, or set an existing author to `None`. Now when a user is deleted, any
posts associated with it remain in place – only their author references are reset to null.

A particularly nifty feature of foreign keys is that they fit nicely into the keyword-based query language we've seen
before. Given several posts, for example, belonging to different users:

```pycon
>>> post1 = await Post(author=alice, content="post 1").save()
>>> post2 = await Post(author=alice, content="post 2").save()
>>> post3 = await Post(author=bob, content="post 3").save()
```

We can effectively traverse table boundaries by specifying the foreign key name (in snake case, i.e. `SomeClass` becomes
`some_class`) and defining conditions on *its* fields:

```pycon
>>> await Post.all(author__name="alice")
[Post(1, title='post 1', author=1), Post(2, title='post 2', author=1)]
>>> await Post.all(author__name__startswith="b")
[Post(3, title='post 3', author=2)]
```

This works even if we have multiple tables that need to be chained; suppose, for example, that each post would further
have comments associated with it:

```pycon
>>> class Comment(Model):
...     post: FK[Post]
...     content: str

>>> await Model.create_table()

>>> comment1 = await Comment(post=post1, comment='comment 1').save()
>>> comment2 = await Comment(post=post2, comment='comment 2').save()
>>> comment3 = await Comment(post=post2, comment='comment 3').save()
```

In this case, we could select all the comments of all of Alice's posts like so:

```pycon
>>> await Comment.all(post__author__name='alice')
[Comment(1, comment='comment 1', post=1), Comment(2, comment='comment 2', post=2)]
```

And all of that without having to do a single `JOIN` ourselves.

### Backreferences and Many-to-Many Relations

What about the reverse? If a post has an author, then an author has many posts; if a comment belongs to a post, then a
post contains many comments. We can similarly query in the opposite direction, by referencing the foreign key's *model*
name *in plural* (not just adding an `s`, mind you: `category` becomes `categories`, `person` becames `people` and so
on):

```pycon
>>> await User.get(posts__title="post 1")
User(1, name='alice')
>>> await User.get(posts__comments__content__endswith="3")
User(2, name='bob')
```

But nice though it is, we're missing actual descriptors (like `post.author`) to easily handle all the user's posts, or
a post's comments. This one-to-many relation can be defined with what's known as a **backreference**:

```pycon
>>> from tunqi import Backref

>>> class User(Model):
...     name: str
...     posts: Backref[Post]

>>> class Post(Model):
...     author: FK[User]
...     title: str
...     comments: Backref[Comment]

>>> class Comment(Model):
...     post: FK[Post]
...     content: str
```

Note that because this relationship is inherently circular, in actual code you'll either have `User` defined before
`Post` (in which case, `Backref[Post]` is probelamtic), or `Post` defined before `User` (in which case, `FK[User]` is);
be sure to do `from __future__ import annotations` to enable such forward references.

Also note that once you define a backreference, *its* name should be used in queries; it usually concides with the
backreferenced model's plural, anyway (`posts`, `comments`, etc.) – but should you decide to get creative and call the
comments backreference `commentary`, then the proper way to select a user who's posts' *commentary* ends with `3` would
be:

```pycon
>>> await User.all(post__commentary__content__endswith="3")
```

In any case, backreferences provide all the methods we've seen so far for a given model – `exists()`, `count()`,
`get()`, `all()`, `create()`, `update()` and `delete()` (in this case there's no ambiguity, so we drop the `_all()`) –
they're already *pr-ewired* to consider only on the subset backreferenced by the particular record on hand:

```pycon
>>> charlie = await User(name="charlie").save()

# Even though we have some posts saved, none of them are Charlie's:
>>> await charlie.posts.exist()
False
>>> await charlie.posts.count()
0

# So let's create a few:
>>> await charlie.posts.create(Post(title="post 4"), Post(title="post 5"))
[4, 5]
>>> await charlie.posts.all()
[Post(4, title='post 4', author=3), Post(5, title='post 5', author=3)]
>>> await charlie.posts.exists()
True
>>> await charlie.posts.count()
2
>>> await charlie.posts.update()(title="post")
2
>>> await charlie.posts.delete()
2
```

What about many-to-many relations? For example, each post might be marked by multiple tags, and each tag might mark
multiple posts. This is usually implemented by a link table, holding pairs of foreign keys that connect a post and a tag
– but with Tunqi, we don't have to worry about it ourselves:

```pycon
>>> from tunqi import M2M

>>> class Tag(Model):
...     name: str
...     posts: M2M[Post]

>>> class Post(Model):
...     title: str
...     tags: M2M[Tag]
```

Again, we get querying abilities:

```pycon
>>> await Post.all(tags__name="tag")   # All the posts tagged with 'tag'
>>> await Tag.all(posts__title="post") # All the tags of post 'post'
```

But more than that, we get a descriptor akin to a (mutual) backreference:

```pycon
>>> post = await Post.get(title="post")
>>> await post.tags.all() # All the tags of post 'post'

>>> tag = await Tag.get(name="tag")
>>> await tag.posts.all() # All the posts tagged with 'tag'
```

The only difference here is that rather than `create`, `update` and `delete`, we have `add` and `remove`, since we're
not actually creating or deleting objects, but rather adding or removing links between them (which have no other fields
that can be updated):

```pycon
>>> post = await Post(title='post').save()
>>> tag1 = await Tag(name='tag 1').save()
>>> tag2 = await Tag(name='tag 2').save()

>>> await posts.tags.add(tag1, tag2)
2
>>> tag1.posts.all()
[Post(1, title='post')]
>>> await tag1.posts.remove(post)
1
>>> await post.tags.all()
[Tag(2, name='tag 2')]
```

And note that duplicates are ignored: if we add a tag that's already associated with a post, nothing happens:

```pycon
>>> await post.tags.add(tag2)
0
```

### Advanced Queries

Going back to querying, let's cover a few more features. Suppose we have multiple users and posts:

```pycon
>>> await User.create(User(name="alice"), User(name="bob"), User(name="charlie"))
[1, 2, 3]
>>> await Post.create([Post(title=f"post {n + 1}", author=n % 3 + 1) for n in range(10)])
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
```

When selecting all of them, we can limit the size of the result set:

```pycon
>>> await Post.all(limit=3)
[Post(1, title='post 1', author=1), Post(2, title='post 2', author=2), Post(3, title='post 3', author=3)]
```

As well as offset it, effectively achieving pagination:

```pycon
>>> await Post.all(offset=3, limit=3)
[Post(4, title='post 4', author=1), Post(5, title='post 5', author=2), Post(6, title='post 6', author=3)]
```

To guarantee a certain order, we can provide the field name we'd like to sort by (or a list thereof):

```pycon
>>> await Post.all(limit=3, order="title")
[Post(1, title='post 1', author=1), Post(2, title='post 2', author=2), Post(3, title='post 3', author=3)]
```

And if we want the sorting to be in descending order, we can prefix that name with `-` (prefixing with `+` results in
ascending order, although this is the default anyway):

```pycon
>>> await Post.all(limit=3, order="-title")
[Post(10, title='post 10', author=1), Post(9, title='post 9', author=3), Post(8, title='post 8', author=2)]
```

To order by fields of a joined table (as well as a nested JSON document), we can use foreign key, backreference or
many-to-many names like we did before – although here, since we're dealing with strings, it's more natural to delimit
them with `.` rather than double underscores:

```pycon
>>> await Post.all(limit=3, order=["author.name", "title"])
[Post(1, title='post 1', author=1), Post(4, title='post 4', author=1), Post(7, title='post 7', author=1)]
```

The same technique used for sorting can be used to fetch particular fields, rather than entire records, using
`get_fields()` (for one result) and `all_fields()` (for many results): providing a string, or a list thereof, returns
dictionaries with those keys only:

```pycon
>>> await Post.all_fields("title", limit=3)
[{'title': 'post 1'}, {'title': 'post 2'}, {'title': 'post 3'}]
```

And delimiting the fields with dots will join tables, like before:

```pycon
>>> await Post.all_fields(["author.name", "title"], limit=3)
[{'author.name': 'alice', 'title': 'post 1'},
 {'author.name': 'bob', 'title': 'post 2'},
 {'author.name': 'charlie', 'title': 'post 3'}]
```

Normally, the selected fields appear as the keys of the corresponding values in the dictionaries, but we can also alias
them by suffixing `:<alias>` to the fields we wish to rename:

```pycon
>>> await Post.all_fields(["author.name:author", "title"], limit=3)
[{'author': 'alice', 'title': 'post 1'}, {'author': 'bob', 'title': 'post 2'}, {'author': 'charlie', 'title': 'post 3'}]
```

Now, just like we can sort by multiple fields and select multiple fields, we might need to query by multiple fields: for
example, selecting all the posts of a certain author that contain some substring. To do that, we simply provide multiple
query keywords:

```pycon
>>> await Post.all(author__name="alice", title__contains="1")
[Post(1, title='post 1', author=1)]
```

Which are applied with `AND` logic; what if we wanted all the posts containing `1` *or* `4`? To do *that*, we'd have
to import the `q` utility, which lets us wrap keywords in objects that can then be combined into compound expressions,
using `~` for `NOT`, `|` for `OR` and `&` for `AND` (although in the last case, providing multiple keywords works just
as well):

```pycon
>>> from tunqi import q
>>> q(x=1) | q(x=2)
<query 'x == 1 or x == 2'>
>>> ~(q(x__gt=1, y__gt=2) | q(x=3))
<query 'not ((x > 1 and y > 2) or x == 3)'>
```

And note that this supports all the tricks we've seen previously:

```pycon
>>> q(author__name__startswith='a') | q(author__name__endswith='b')
<query "author.name starting with 'a' or author.name ending with 'b'">
```

To use this as an actual query, we must pass it in via the special `where=` keyword:

```pycon
>>> await Post.all(author__name="alice", where=q(title__contains="1") | q(title__contains="4"))
[Post(1, title='post 1', author=1), Post(4, title='post 4', author=1)]
```

### Database Management

There are more advanced features to cover, but before we do, let's step back and talk about databases. So far, we've
created one default database, which was automatically applied to all our models' operations without us having to touch
it. In fact, there's one prominent use case in which we have to: **transactions**, which guarantee that the operations
that happen within their scope either happen together or don't happen at all.

To get the currently active database, we do:

```pycon
>>> db = Database.get()
>>> db
<Database at 'sqlite:///:memory:/'>
```

And to start a transaction:

```pycon
>>> with db.transaction():
...     ...
```

If this context ends successfully, all the changes that occured in it are **committed**; if it exits with an error, all
of them are **rolled back**. When we open one transaction inside another, it has no effect – after all, if the inner one
fails, whatever changes it made are invalid, so the outer one can't complete either; and if it succeeds, it's still up
to the outer one to finish successfully.

In cases where we do want to delineate such "units of work", we can pass in `nested=True`, which will create a
**savepoint**: essentially, if the inner transaction fails, *its* changes will be reverted; but as long as the error is
caught and handled in the outer transaction, its own changes can be applied as planned.

But what is that *active* database I referred to? If there's just one default database, the answer is obvious; but if
we're dealing with multiple databases, we can set one or the other as temporarily active using a context:

```pycon
>>> db1 = Database(...)
>>> db2 = Database(...)
>>> with db1:
...     # db1 is active
>>> with db2:
...     # db2 is active
```

Which takes effect throughout the current task or thread. To apply such a change globally, we either pass in
`default=True` when creating the database, as we have seen, or call it later explicitly:

```pycon
>>> db.set_default()
```

This lasts until another database takes its place, or until we're done with it:

```pycon
>>> await db.stop()
```

After which point it's no longer operational – further operations will fail, unless we set a new default, or temporarily
activate one.

Finally, we can also create databases dynamically. This is harder to exemplify with SQLite, since in it each database is
a separate file, so all it takes to create a new one is specifying a different path – but in PostgreSQL and MySQL, we do
so with:

```pycon
>>> another_db = await db.create_database('<name>')
```

Which gives us a new handle, pointing to a newly created namespace, which we can later remove:

```pycon
>>> await another_db.drop_database()
```

When we do that, it goes ahead and `stop()`s that database as well, since there's nothing else that can be done through
it anyway.

### Migrations

So far we've only seen one way to create tables: `Model.create_tables()`, which defines the schemas of all of `Model`'s
subclasses (ignoring tables that already exist); unsurprisingly, its counterpart is `Model.drop_tables()`, which deletes
them.

However, a more robust approach is **migration**: a comparison of what schemas exist in the database to how they look in
our code, which genereates a script that, when run, will reconcile any detected differences. This uses the popular
`alembic` library – although, it usually requires creating and managing a bunch of awkward files, so in our case the
same mechanism is exposed via a simpler interface:

```pycon
>>> await Model.make_migrations('<migrations-directory>')
```

This compares the schemas and generates the migration script in the specified directory; then:

```pycon
>>> await Model.migrate('<migrations-directory>')
```

Applies the relevant scripts from that directory, bringing the database up to date. Note that in between, you might want
to view and edit those scripts – renaming columns, for example, is something that `alembic` can't detect on its own, so
by default it drops the old columns and adds new ones, which might result in a data loss; and generally, since we're
dealing with automatic inference and code generation, it's always better to double-check and adjust.

### Model Management

Inheritance is not terribly common in SQL, but in Pydantic it is: having a base class define some shared functionality
that is then included in its subclasses is common practice, which Tunqi can accommodate by adding `abstract=True` to
class definition:

```pycon
>>> from datetime import datetime
>>> from pydantic import Field

>>> class Base(Model, abstract=True):
...     created: datetime = Field(default_factory=datetime.now)
...     updated: datetime = Field(default_factory=datetime.now)

>>> class User(Base):
...     name: str

>>> class Post(Base):
...     author: FK[User]
...     title: str

>>> user = await User(name="alice").save()
>>> user
User(1, created=..., updated=..., name='alice')

>>> post = await Post(author=user, title="Hello, world!").save()
>>> post
Post(1, created=..., updated=..., title='Hello, world!', author=1)
```

Without `abstract=True`, a `base` table would be created, and we'd be able to save and delete `Base()` instances, which
doesn't make much sense. With it, the class is only retained as a Python container of shared functionality, which is
usually what we want.

#### Lifecycle Methods

Besides attributes, base classes are a good place to define common methods, which is a good time to discuss the ones
pertaining to a model's **lifecycle**:

- `before_create()` is called before a record is created;
- `after_create()` is called after a record is created;
- `before_update()` is called before a record is updated;
- `after_update()` is called after a record is updated;
- `before_save()` is called before a record is saved (and before `before_create` or `before_update`, if it matters);
- `after_save()` is called after a record is saved (and after `after_create` or `after_update`);
- `before_delete()` is called before a record is deleted;
- `after_delete()` is called after a record is deleted.

Note that when records are created, updated or deleted, these happen as part of a transaction – so if *either* their
`before_` or `after_` methods raise an error, the entire operation is aborted. For batch operations, it also takes care
of restoring the objects to their original state (so if the last object in a batch-create fails – none are created, and
all their `pk`s remain `None`).

The reason I mention it now is that I'd actually implement our previous example a bit differently:

```pycon
>>> class Base(Model, abstract=True):
...     created: datetime | None = None
...     updated: datetime | None = None
...
...     async def before_create(self) -> None:
...         self.created = datetime.now()
...
...     async def before_save(self) -> None:
...         self.updated = datetime.now()
```

That is: set the creation time to the moment the object is saved for the first time (rather than the time its instance
is initialized); and synchronoize its update timestamp every time it is saved, regardless of whether it's the first time
or not.

#### Fixed Queries

Another such interesting method (although it doesn't pertain to lifecycle per say) is `model_query`. Suppose each
resource in our system belongs to one organization or another:

```pycon
>>> class Organization(Model):
...     name: str

>>> class Resource(Model, abstract=True):
...     organization: FK[Organization]
...     async def before_create(self) -> None:
...         self.organization = get_current_organization()

>>> class Document(Resource):
...     title: str
...     content: str
```

Each class that inherits from `Resource` will automatically gain the `organization` foreign key, and be associated with
the current organization (e.g. depending on what how the user is logged in) upon creation. Then, however, each time we
want to fetch resources *for the current organization*, we'd have to include it:

```pycon
>>> return await Document.all(content__contains="...", organization__pk=get_current_organization().pk)
```

Because if we don't, users from other organizations will have access to resources belonging to this one. So, to make
sure this clause is *always* applied (at least, to any subclass of `Resource`), we can do:

```pycon
>>> class Resource(Model, abstract=True):
...     ... # Same as before
...     @classmethod
...     async def model_query(cls) -> dict[str, Any]:
...         return {'organization__pk': get_current_organization().pk}
```

Which will include this additional query in any operation where queries are applied.

#### Model Subtrees

Once our inheritance trees become more elaborate, we might find ourselves wanting to separate different branches of it.
In this case, instead of calling `Model.create_tables()` or `Model.make_migrations()` – `Model` being the abstract base
class of *all* models, and hence creating or migrating *all of them* – we can do:

```pycon
>>> await Resource.create_tables()
# Or...
>>> await Resource.make_migrations('<migrations-directory>')
```

Which will create or migrate (or drop, for that matter) only the models that make up this subtree. We can even associate
such subtrees with different databases – like in a case where we have one database for operational data, and one for
research or telemetries. Having two corresponding base classes:

```pycon
>>> class OperationalModel(Model, abstract=True):
...     pass

>>> class ResearchModel(Model, abstract=True):
...     pass
```

And inheriting from *them* rather than from `Model`, will not only let us manage their subclasses separately, but also
do:

```pycon
>>> operational_db = Database(...)
>>> research_db = Database(...)
>>> OperationalModel.use(operational_db)
>>> ResearchModel.use(research_db)
```

Which guarantees that, regardless of the default or temporarily active database, models of *this* kind will use *that*
particular database.

#### Deduplication

One more model-related option I'd like to cover is **deduplication**: making sure that when a record of the same PK
is retrieved, it's always the same object, so even if one already exists – it is reused, with its state "refreshed"
to this most recent version. This is done by passing `deduplicate=True` when defining a class:

```pycon
>>> class User(Model, deduplicate=True):
...     name: str

>>> user1 = await User(name="alice").save()

>>> user2 = await User.get(name="alice")
>>> user1 is user2
True

>>> await User.update(name="alice")(name="bob")
>>> user3 = await User.get(name="bob")
>>> user1 is user3
True
>>> user1.name
'bob'
```

And just like setting a database, this option applies to all the subclasses of a model where it was provided. To wire
particular models differently, we can always do:

```pycon
>>> User.config.set_database(other_db)
# And...
>>> User.config.set_deduplication(True/False)
```

Which will similarly apply to *their* subtree – but that's a rare edge-case, really.

#### Customizing Table Names and Plural Nouns

Another edge-case is customizing the name of the table (which defaults to the model's name in snake case) or its plural
noun (used in queries when backreferences are not defined explicitly) – this is similarly done with class arguments:

```pycon
>>> class Person(Model, table_name='personTable', plural='persons'):
...     ...
```

But honestly, I don't know why you would.

### Unique Constraints

To make a column unique, all we have to do is wrap its annotation as such:


```pycon
>>> from tunqi import Unique

>>> class User(Model):
...     name: Unique[str]
```

If we want several columns to be unique *together*, calling the special `unique` function *inside the class body* will
take care of it for us:

```pycon
>>> class User(Model):
...     organization: FK[Organization]
...     email: str
...     unique("email", "organization") # Email must be unique per-organization
```

Such constraints are stored in `User.config.unique`, and if they're ever violated, a normalized, informative
`AlreadyExistsError` is raised – regardless of whether we're using SQLite, PostgreSQL or MySQL (which you'd think is
obvious – but they all have annoying different and indirect errors for this common and sensible case!).

That said, MySQL has an annoying limitation: its strings cannot be unique unless their maximum length is specified. The
strings we've seen so far were all saved as `TEXT` – so to accommodate this constraint, and generally allow us to use
`VARCHAR` when necessary, some special syntax is warranted. So far, I haven't found a way to make it more elegant than:

```pycon
>>> from tunqi import length
>>> from typing import Annotated

>>> class User(Model):
...     name: Unique[Annotated[str, length(255)]]
```

You see, doing something like `String[255]` makes MyPY complain, while doing something like `String(255)` annoys
PyLance. So, we're stuck with the somewhat inelegant `Annotated[<type>, <config>]` compromise – but we make the best of
it, as we'll see later when we talk about how to extend the ORM with new, custom types.

### Indexing

Indexing speeds up queries by a particular column; there's really not much else to say about it. All we do is:

```pycon
>>> from tunqi import Index

>>> class User(Model):
...     name: Index[str]
```

And of course, we can compose it with `Unique` (as well as `| None`, which we've seen to make a column nullable):

```pycon
>>> class User(Model):
...     name: Index[Unique[str | None]]
```

As an aside, indexing doesn't make much sense on JSON columns – at least, not for SQLite and MySQL; for PostgreSQL, this
is taken to mean the special GIN index, which does increase performance for traversing nested documents.

### JSON Support

Having mentioned that, we should probably talk about JSON. Whenever we have an attribute that's not a basic type –
primarily lists, dictionaries, nested Pydantic models or a combination thereof – everything works seamlessly:

```pycon
>>> class Address(BaseModel): # Note: this is not a table!
...     country: str
...     city: str
...     street: str
...     apartment: str

>>> class User(Model):
...     name: str
...     addresses: list[Address]

>>> address1 = Address(country="Spain", city="Madrid", street="Gran Via", apartment=1)
>>> address2 = Address(country="France", city="Paris", street="Champs-Elysees", apartment=2)
>>> await User(name="alice", addresses=[address1, address2]).save()
User(1, name='alice', addresses=[...])

>>> user = await User.get(name="alice")
>>> user.addresses[0].country
'Spain'
```

And if we want to select by the nested fields of a column such as `addresses`, we do so by chaining the dictionary keys
or list indices to our query keywords:

```pycon
>>> await User.all(addresses__0__country='Spain') # Users whose primary address is in Spain:
[User(1, name='alice', addresses=[...])]
```

If we'd want to select users with *any* or *all* logic applied to their JSON columns (e.g., users that have *some*
address in Spain, not necessarily their first) – this is not currently supported, but I have an idea on how to do it, so
stay tuned.

In any case, using JSON with SQL is a tremendously useful paradigm – one that renders NoSQL alternatives such as MongoDB
pretty moot, since SQL is so much more stable and efficient – so having native, first-rate support for nested documents
within an otherwise flat data model was part of the reason this library was even concieved, and I encourage you to use
it with gusto.

### Upserting

Another interesting use-case is **upserting** – creating a record if it doesn't exist, or updating it if it does; or,
similarly, `get_or_create()`, which returns a record, creating it if it isn't present. The latter has a simple, handy
method:

```
>>> user1 = await User.get_or_create(name="alice")
>>> user1
User(1, name="alice")
>>> user2 = await User.get_or_create(name="alice")
User(1, name="alice") # Same user
>>> user3 = await User.get_or_create(name="bob")
User(2, name="bob") # New user
```

Whereas the former is a bit more complex. Essentially, we're doing a regular `create()` operation – but by providing the
`on_conflict=` keyword, which lists the field names that should be monitored for clashes, we're telling the ORM that
special care must be taken if such fields with similar values already exist. The policy to handle it, then, is defined
with the `update=` keyword: `False` for "do nothing" (abort the `INSERT` statement); `True` for a full update (replace
the record); and a field name or list thereof if only certain parts should be replaced:

```pycon
>>> class User(Model):
...     name: str
...     email: str
...     age: int

>>> alice = User(name="alice", email="alice@example.com", age=25)
# If a user with this name exists, do nothing:
>>> await User.create(alice, on_conflict="name", update=False)
# If a user with this name and email exists, update it:
>>> await User.create(alice, on_conflict=["name", "email"], update=True)
# If a user with this email exists, update its name (but not age):
>>> await User.create(alice, on_conflict="email", update="name")
```

### Distinct Counts

Another cool thing we can do – this time, when counting – is only ask about *distinct* occurences or a certain field 
(or a list thereof). For example, if our users' names are not unique, but their emails are:

```pycon
>>> class User(Model):
...     name: str
...     email: Unique[str]

>>> user1 = await User(name="alice", email="alice@example.com").save()
>>> user2 = await User(name="alice", email="alice@gmail.com").save()
>>> user3 = await User(name="bob", email="bob@example.com").save()
```

Then if we count them, we'll naturally get 3:

```pycon
>>> await User.count()
3
```

But if we count their *names*, there are only two – `alice` and `bob`, right?

```pycon
>>> await User.count("name")
2
```

### Aggregations

### Functions

### Column References

### Extending The Query Language

### Extending the Type System

### Escaping into SQL

If, for some reason, all of these features are not enough for what you want to do – don't worry: you can always run a
raw SQL statement (potentially binding it to some values, and getting back an `SQLAlchemy` cursor) with:

```pycon
>>> async with db.execute('<statement>', **values) as cursor:
...     ...
```

And if this still isn't enough, you can escape back into the comfort of `SQLAlchemy` – on top of which this library is
built – and do anything at all "the old way":

```pycon
>>> engine = db.engine
>>> users = User.get_table()
>>> ... # Good ol' SQLAlchemy code (at least, SQLAlchemy core).
```

### Using the Database Directly

### Auditing