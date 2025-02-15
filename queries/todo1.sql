select
    3*4 as a,
    3*5as b -- bigquery does not like this
    "5"as c -- bigquery likes this (todo: modify the scanner to have the same behavior)
