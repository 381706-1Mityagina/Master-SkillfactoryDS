create table if not exists product (
  product_id_unique serial UNIQUE PRIMARY KEY NOT NULL,
  product_id integer,
  brand varchar,
  product_line varchar,
  product_class varchar,
  product_size varchar,
  list_price float8,
  standard_cost float8
)

create table if not exists customer (
  customer_id integer UNIQUE PRIMARY KEY NOT NULL,
  first_name varchar,
  last_name varchar,
  gender varchar,
  DOB varchar,
  job_title varchar,
  job_industry_category varchar,
  wealth_segment varchar,
  deceased_indicator varchar,
  owns_car boolean,
  address text NOT NULL,
  postcode varchar,
  state varchar,
  country varchar,
  property_valuation integer
)

create table if not exists transaction (
  transaction_id integer UNIQUE PRIMARY KEY NOT NULL,
  product_id_unique serial,
  product_id integer,
  customer_id integer,
  transaction_date varchar,
  online_order boolean,
  order_status varchar
)

alter table transaction add foreign key (product_id_unique) references product (product_id_unique);

alter table transaction add foreign key (customer_id) references customer (customer_id);
