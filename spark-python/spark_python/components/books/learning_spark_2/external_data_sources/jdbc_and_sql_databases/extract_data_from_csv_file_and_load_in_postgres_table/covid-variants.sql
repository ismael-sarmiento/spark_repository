-- public.covid_variants

-- drop table
drop table if exists public.covid_variants;

-- create table
CREATE TABLE if not exists public.covid_variants (
	location VARCHAR(25) NOT NULL,
	_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
	variant VARCHAR(255) NOT NULL,
	num_sequences INTEGER NOT NULL,
	perc_sequences DECIMAL(5, 5) NOT NULL,
	num_sequences_total INTEGER NOT NULL
);
