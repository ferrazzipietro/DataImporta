create database dataimporta;
\c dataimporta;

create table all_countries (
    country_of_arrival VARCHAR(100), 
    mean_of_transport VARCHAR(150), 
    price_transport_net VARCHAR(100), 
    price_transport_net_insurance VARCHAR(100), 
    net_price_per_unit VARCHAR(100), 
    commercial_description VARCHAR(500), 
    custom VARCHAR(100), 
    date VARCHAR(75), 
    net_price VARCHAR(100), 
    country VARCHAR(20), 
    type CHAR(3) );


CREATE MATERIALIZED VIEW peru_imp
AS
select * from all_countries where country = 'peru' and type = 'IMP';

CREATE MATERIALIZED VIEW chile_imp
AS
select * from all_countries where country = 'chile' and type = 'IMP';

CREATE MATERIALIZED VIEW brazil_imp
AS
select * from all_countries where country = 'brazil' and type = 'IMP';

