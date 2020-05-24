# RQLMongo

[![Build Status](https://travis-ci.org/pjwerneck/rqlmongo.svg?branch=develop)](https://travis-ci.org/pjwerneck/rqlmongo)

Resource Query Language extension for PyMongo

## Overview

Resource Query Language (RQL) is a query language designed for use in
URIs, with object-style data structures.

rqlmongo is an RQL extension for PyMongo. It translates an RQL query
to a MongoDB aggregation pipeline that can be used to expose MongoDB
collections as an HTTP API endpoint and perform complex queries using
only querystring parameters.
