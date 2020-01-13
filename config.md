The schema defines the following properties:

# `logLevel` (string, enum)

This element must be one of the following enum values:

* `error`
* `warning`
* `info`
* `debug`
* `trace`

Default: `"info"`

# `disconnectOnError` (boolean)

Default: `true`

# `id` (string)

# `type` (string)

# `namespace`

The object must be one of the following types:

* `undefined`
* `undefined`

# `imports` (array)

The elements of the array must match *exactly one* of the following properties:

# (string)

# (object)

# `metrics`

The object must be one of the following types:

* `undefined`
* `undefined`

# `connection` (object, required)

Properties of the `connection` object:

## `server` (string, required)

## `database` (string, required)

## `user` (string, required)

## `password` (string, required)

## `connectionTimeout` (integer,null)

## `requestTimeout` (integer,null)