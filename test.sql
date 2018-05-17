-- 'DBNAME': 'mqdashboard',
-- 'USER': 'dbreader',
-- 'PASSWORD': 'cmcrc2018!',
-- 'SERVER': 'mqdashboarddb-metrics.czm2hxmcygx4.us-east-1.rds.amazonaws.com',
select distinct m.uptick_name, ds.security, t.currency_id from api_dailystats ds
join api_tradable t on (ds.tradable_id = t.id)
join api_market m on (ds.market_id = m.id)
where ds.market_id in (1, 3, 33, 38, 208)
and ds.date between '2018-04-06' and '2018-05-03' LIMIT 10;

select * from api_market a where a.uptick_name = ANY(array['asx','sgx','johannesburg','istanbul','sao_paulo']);

select distinct trading_market as Market, symbol as RIC from
refdata_tradablesymbolmap where trading_market in ('asx', 'sgx',
'istanbul', 'johannesburg', 'sao_paulo') and tradable in
('sgx:RESH:SGD', 'sao_paulo:NVHO11:BRL', 'asx:BSX:AUD',
'johannesburg:SOHJ:ZAC', 'sgx:TECK:SGD', 'sao_paulo:DWDP34:BRL',
'sao_paulo:MSCD34:BRL', 'asx:AYI:AUD', 'asx:PMY:AUD',
'asx:BEL:AUD', 'sgx:MDRT:SGD') and date between '2018-04-06' and
'2018-05-03' limit 10;


-- dangerous user
-- 'ENGINE': 'django.db.backends.postgresql_psycopg2',
-- 'NAME': 'mqdashboard',
-- 'USER': 'mqdashboard',
-- 'PASSWORD': 'I99ub6Lw',
-- 'HOST': 'mqdashboarddb-metrics.czm2hxmcygx4.us-east-1.rds.amazonaws.com',



-- 'dbname': 'refdata',
-- 'host': 'reference-data.czm2hxmcygx4.us-east-1.rds.amazonaws.com',
-- 'port': '5432',
-- 'user': 'reader',
-- 'password': 'refdatareader2017!'
