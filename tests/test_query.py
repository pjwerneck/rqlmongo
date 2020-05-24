import itertools
from operator import itemgetter

import pytest

from rqlmongo import RQLMongo
from rqlmongo import RQLQueryError


class TestQuery:
    def test_simple_sort(self, db, users):
        res = RQLMongo(db.users).rql("sort(balance)")
        exp = sorted(users, key=itemgetter("balance"))
        assert list(res) == exp

    def test_simple_sort_desc(self, db, users):
        res = RQLMongo(db.users).rql("sort(-balance)")
        exp = sorted(users, key=itemgetter("balance"), reverse=True)
        assert list(res) == exp

    def test_complex_sort(self, db, users):
        res = RQLMongo(db.users).rql("sort(balance,registered,birthdate)")
        exp = sorted(users, key=itemgetter("balance", "registered", "birthdate"))
        assert list(res) == exp

    def test_in_operator(self, db, users):
        res = RQLMongo(db.users).rql("in(state,(FL,TX))")
        exp = [u for u in users if u["state"] in {"FL", "TX"}]

        assert list(res) == exp

    def test_out_operator(self, db, users):
        res = RQLMongo(db.users).rql("out(state,(FL,TX))")
        exp = [u for u in users if u["state"] not in {"FL", "TX"}]

        assert list(res) == exp

    def test_contains_string(self, db, users):
        res = RQLMongo(db.users).rql("contains(email,besto.com)")
        exp = [u for u in users if "besto.com" in u["email"]]

        assert list(res) == exp

    def test_excludes_string(self, db, users):
        res = RQLMongo(db.users).rql("excludes(email,besto.com)")
        exp = [u for u in users if "besto.com" not in u["email"]]

        assert list(res) == exp

    def test_contains_array(self, db, users):
        res = RQLMongo(db.users).rql("contains(tags,aliqua)")
        exp = [u for u in users if "aliqua" in u["tags"]]

        assert list(res) == exp

    def test_excludes_array(self, db, users):
        res = RQLMongo(db.users).rql("excludes(tags,aliqua)")
        exp = [u for u in users if "aliqua" not in u["tags"]]

        assert list(res) == exp

    def test_limit(self, db, users):
        res = RQLMongo(db.users).rql("limit(2)")
        exp = users[:2]

        assert list(res) == exp

    def test_select_no_id(self, db, users):
        res = RQLMongo(db.users).rql("select(city,state)")
        exp = [{k: u[k] for k in ("city", "state")} for u in users]

        assert list(res) == exp

    def test_select_with_id(self, db, users):
        res = RQLMongo(db.users).rql("select(_id,city,state)")
        exp = [{k: u[k] for k in ("_id", "city", "state")} for u in users]

        assert list(res) == exp

    def test_values(self, db, users):
        res = RQLMongo(db.users).rql("values(state)")
        exp = [{"values": [u["state"] for u in users]}]

        assert list(res) == exp

    def test_sum(self, db, users):
        res = RQLMongo(db.users).rql("sum(balance)")
        exp = [{"_id": 1, "balance": pytest.approx(sum([u["balance"] for u in users]))}]

        assert list(res) == exp

    def test_mean(self, db, users):
        res = RQLMongo(db.users).rql("mean(balance)")
        exp = [{"_id": 1, "balance": pytest.approx(sum([u["balance"] for u in users]) / len(users))}]

        assert list(res) == exp

    def test_max(self, db, users):
        res = RQLMongo(db.users).rql("max(balance)")
        exp = [{"_id": 1, "balance": max([u["balance"] for u in users])}]

        assert list(res) == exp

    def test_min(self, db, users):
        res = RQLMongo(db.users).rql("min(balance)")
        exp = [{"_id": 1, "balance": min([u["balance"] for u in users])}]

        assert list(res) == exp

    def test_first(self, db, users):
        res = RQLMongo(db.users).rql("first()")
        exp = [users[0]]

        assert list(res) == exp

    def test_one(self, db, users):
        res = list(RQLMongo(db.users).rql("guid=658c407c-6c19-470e-9aa6-8c2b86cddb4b&one()"))
        exp = [u for u in users if u["guid"] == "658c407c-6c19-470e-9aa6-8c2b86cddb4b"]

        assert len(res) == 1
        assert res == exp

    def test_one_no_results(self, db, users):
        with pytest.raises(RQLQueryError) as exc:
            RQLMongo(db.users).rql("guid=lero&one()")

        assert exc.value.args[0] == "No result found for one()"

    def test_one_multiple_results(self, db, users):
        with pytest.raises(RQLQueryError) as exc:
            RQLMongo(db.users).rql("state=FL&one()")

        assert exc.value.args[0] == "Multiple results found for one()"

    def test_distinct(self, db, users):
        res = list(RQLMongo(db.users).rql("select(gender)&distinct()"))

        assert len(res) == 2
        assert {"gender": "female"} in res
        assert {"gender": "male"} in res

    def test_count(self, db, users):
        res = RQLMongo(db.users).rql("count()")
        exp = [{"count": len(users)}]
        assert list(res) == exp

    def test_distinct_count(self, db, users):
        res = RQLMongo(db.users).rql("select(gender)&distinct()&count()")
        assert list(res) == [{"count": 2}]

    @pytest.mark.parametrize("index", (1, 2, 3))
    def test_eq_operator(self, db, users, index):
        res = RQLMongo(db.users).rql(f"index={index}")
        exp = [u for u in users if u["index"] == index]

        assert list(res) == exp

    @pytest.mark.parametrize("balance", (1000, 2000, 3000))
    def test_gt_operator(self, db, users, balance):
        res = RQLMongo(db.users).rql(f"gt(balance,{balance})")
        exp = [u for u in users if u["balance"] > balance]

        assert list(res) == exp

    def test_aggregate(self, db, users):
        res = RQLMongo(db.users).rql("aggregate(state,gender,sum(balance))")

        res = [{**d, "balance": round(d["balance"], 2)} for d in res]

        exp = []

        key = itemgetter("state", "gender")

        for (state, gender), group in itertools.groupby(sorted(users, key=key), key=key):
            exp.append({"state": state, "gender": gender, "balance": round(sum([u["balance"] for u in group]), 2)})

        res.sort(key=key)
        exp.sort(key=key)

        assert len(res) == len(exp)
        assert list(res) == exp

    def test_aggregate_with_filter(self, db, users):
        res = RQLMongo(db.users).rql("gender=male&aggregate(state,sum(balance))")

        res = [{**d, "balance": round(d["balance"], 2)} for d in res]

        exp = []

        key = itemgetter("state")

        for state, group in itertools.groupby(sorted(users, key=key), key=key):
            exp.append(
                {"state": state, "balance": round(sum([u["balance"] for u in group if u["gender"] == "male"]), 2)}
            )

        res.sort(key=key)
        exp.sort(key=key)

        assert len(res) == len(exp)
        assert list(res) == exp
