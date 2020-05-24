import datetime
import re

from pyrql import RQLSyntaxError
from pyrql import parse

OPERATORS = {
    "ge": "gte",
    "le": "lte",
}


class RQLQueryError(Exception):
    pass


class RQLMongo:
    _rql_error_cls = RQLQueryError

    _rql_max_limit = None
    _rql_default_limit = None
    _rql_auto_scalar = False

    def __init__(self, collection):
        self._collection = collection

    def rql_to_pipeline(self, query, limit=None):
        if not query:
            self.rql_parsed = None
            self.rql_expr = ""

        else:
            self.rql_expr = query

            try:
                self.rql_parsed = parse(query)
            except RQLSyntaxError as exc:
                raise self._rql_error_cls("RQL Syntax error: %r" % (exc.args,))

        self._rql_one_clause = None

        self._pipeline = []

        self._rql_walk(self.rql_parsed)

        return self._pipeline

    def rql(self, query, limit=None):
        pipeline = self.rql_to_pipeline(query, limit=limit)

        if self._rql_one_clause:
            cursor = self._collection.aggregate(pipeline + [{"$count": "count"}])
            try:
                count = next(cursor)["count"]
            except StopIteration:
                count = 0

            if count > 1:
                raise RQLQueryError("Multiple results found for one()")
            if count < 1:
                raise RQLQueryError("No result found for one()")

        cursor = self._collection.aggregate(pipeline)
        return cursor

    def _rql_walk(self, node):
        # filtering nodes will be used by the where clause. Other
        # nodes will be added to the pipeline separately by the
        # visitor methods below
        if node:
            match = self._rql_apply(node)
            if match:
                self._pipeline.insert(0, {"$match": match})

    def _rql_apply(self, node):
        if isinstance(node, dict):
            name = node["name"]
            args = node["args"]

            if name in {"eq", "ne", "lt", "le", "gt", "ge"}:
                return self._rql_cmp(args, OPERATORS.get(name, name))

            try:
                method = getattr(self, "_rql_" + name)
            except AttributeError:
                raise self._rql_error_cls("Invalid query function: %s" % name)

            return method(args)

        elif isinstance(node, list):
            raise NotImplementedError

        elif isinstance(node, tuple):
            raise NotImplementedError

        return node

    def _rql_attr(self, attr):
        return attr

    def _rql_value(self, value, attr=None):
        if isinstance(value, dict):
            value = self._rql_apply(value)

        return value

    def _rql_binop(self, args, op):
        attr, value = args

        attr = self._rql_attr(attr)
        value = self._rql_value(value, attr)
        return {attr: {"$" + op: value}}

    def _rql_cmp(self, args, op):
        return self._rql_binop(args, op)

    def _rql_and(self, args):
        args = [self._rql_apply(node) for node in args]
        args = [a for a in args if a is not None]
        if args:
            return {"$and": args}

    def _rql_or(self, args):
        args = [self._rql_apply(node) for node in args]
        args = [a for a in args if a is not None]
        if args:
            return {"$or": args}

    def _rql_in(self, args):
        return self._rql_binop(args, "in")

    def _rql_out(self, args):
        return self._rql_binop(args, "nin")

    def _rql_contains(self, args):
        attr, value = args
        attr = self._rql_attr(attr)
        value = self._rql_value(value, attr)
        rx = re.compile(value)

        return {attr: rx}

    def _rql_excludes(self, args):
        attr, value = args
        attr = self._rql_attr(attr)
        value = self._rql_value(value, attr)
        rx = re.compile(value)

        return {attr: {"$not": rx}}

    def _rql_select(self, args):
        attrs = [self._rql_attr(attr) for attr in args]
        project = {a: 1 for a in attrs}
        # mongo projections include _id by default.
        project.setdefault("_id", 0)
        self._pipeline.append({"$project": project})

    def _rql_values(self, args):
        (attr,) = args
        attr = self._rql_attr(attr)
        self._pipeline.extend(
            [
                {"$project": {attr: f"${attr}"}},
                {"$group": {"_id": None, "values": {"$push": f"${attr}"}}},
                {"$project": {"_id": 0}},
            ]
        )

    def _rql_distinct(self, args):
        self._pipeline.extend(
            [
                {"$group": {"_id": None, "doc": {"$addToSet": "$$ROOT"}}},
                {"$unwind": "$doc"},
                {"$replaceRoot": {"newRoot": "$doc"}},
            ]
        )

    def _rql_sum(self, args):
        (attr,) = args
        attr = self._rql_attr(attr)
        self._pipeline.append({"$group": {attr: {"$sum": "$" + attr}, "_id": 1}})

    def _rql_mean(self, args):
        (attr,) = args
        attr = self._rql_attr(attr)
        self._pipeline.append({"$group": {attr: {"$avg": "$" + attr}, "_id": 1}})

    def _rql_min(self, args):
        (attr,) = args
        attr = self._rql_attr(attr)
        self._pipeline.append({"$group": {attr: {"$min": "$" + attr}, "_id": 1}})

    def _rql_max(self, args):
        (attr,) = args
        attr = self._rql_attr(attr)
        self._pipeline.append({"$group": {attr: {"$max": "$" + attr}, "_id": 1}})

    def _rql_limit(self, args):
        args = [self._rql_value(v) for v in args]

        limit = min(args[0], self._rql_max_limit or 2 ** 31)

        if len(args) == 2:
            self._pipeline.append({"$skip": args[2]})

        self._pipeline.append({"$limit": limit})

    def _rql_sort(self, args):
        # normalize sort args with '+'
        args = [("+", v) if isinstance(v, str) else v for v in args]
        # pair signals with attributes
        args = [(p, self._rql_attr(v)) for (p, v) in args]

        attrs = {attr: -1 if p == "-" else 1 for (p, attr) in args}

        self._pipeline.append({"$sort": attrs})

    def _rql_count(self, args):
        self._pipeline.append({"$count": "count"})

    def _rql_first(self, args):
        self._pipeline.append({"$limit": 1})

    def _rql_one(self, args):
        self._rql_one_clause = True

    def _rql_time(self, args):
        return datetime.time(*args)

    def _rql_date(self, args):
        return datetime.date(*args)

    def _rql_dt(self, args):
        return datetime.datetime(*args)

    def _rql_aggregate(self, args):
        attrs = []
        aggrs = []

        for x in args:
            if isinstance(x, dict):
                agg_func = x["name"]
                agg_attr = self._rql_attr(x["args"][0])
                aggrs.append((agg_func, agg_attr))

            else:
                attrs.append(self._rql_attr(x))

        group = {}
        group["_id"] = {field: "$" + field for field in attrs}
        for field in attrs:
            group[field] = {"$first": "$" + field}

        for func, field in aggrs:
            group[field] = {"$" + func: "$" + field}

        pipeline = [{"$group": group}, {"$project": {"_id": 0}}]

        self._pipeline.extend(pipeline)
