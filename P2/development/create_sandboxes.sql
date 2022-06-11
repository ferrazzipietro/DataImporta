CREATE MATERIALIZED VIEW peru_imp
AS
select * from all_countries where country = 'peru' and type = 'IMP';

CREATE MATERIALIZED VIEW chile_imp
AS
select * from all_countries where country = 'chile' and type = 'IMP';

CREATE MATERIALIZED VIEW brazil_imp
AS
select * from all_countries where country = 'brazil' and type = 'IMP';

