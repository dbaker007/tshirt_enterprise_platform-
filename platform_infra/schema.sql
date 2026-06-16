CREATE TABLE communication_ledger (
	id SERIAL NOT NULL, 
	order_id VARCHAR, 
	customer_name VARCHAR, 
	status VARCHAR, 
	created_at TIMESTAMP WITHOUT TIME ZONE, 
	PRIMARY KEY (id)
);

CREATE TABLE customers (
	id SERIAL NOT NULL, 
	name VARCHAR, 
	email VARCHAR, 
	PRIMARY KEY (id)
);

CREATE TABLE finance_ledger (
	id SERIAL NOT NULL, 
	order_id VARCHAR, 
	status VARCHAR, 
	created_at TIMESTAMP WITHOUT TIME ZONE, 
	PRIMARY KEY (id)
);

CREATE TABLE finance_outbox (
	id SERIAL NOT NULL, 
	topic VARCHAR NOT NULL, 
	key VARCHAR NOT NULL, 
	payload VARCHAR NOT NULL, 
	trace_context VARCHAR, 
	created_at TIMESTAMP WITHOUT TIME ZONE, 
	PRIMARY KEY (id)
);

CREATE TABLE invoices (
	id SERIAL NOT NULL, 
	order_id VARCHAR, 
	customer_id INTEGER, 
	amount FLOAT, 
	PRIMARY KEY (id)
);

CREATE TABLE notification_outbox (
	id SERIAL NOT NULL, 
	topic VARCHAR NOT NULL, 
	key VARCHAR NOT NULL, 
	payload VARCHAR NOT NULL, 
	trace_context VARCHAR, 
	created_at TIMESTAMP WITHOUT TIME ZONE, 
	PRIMARY KEY (id)
);

CREATE TABLE sales_outbox (
	id SERIAL NOT NULL, 
	topic VARCHAR NOT NULL, 
	key VARCHAR NOT NULL, 
	payload VARCHAR NOT NULL, 
	trace_context VARCHAR, 
	created_at TIMESTAMP WITHOUT TIME ZONE, 
	PRIMARY KEY (id)
);

CREATE TABLE saga_states (
	order_id VARCHAR NOT NULL, 
	status VARCHAR NOT NULL, 
	finance_status VARCHAR, 
	shipping_status VARCHAR, 
	notifications_status VARCHAR, 
	created_at TIMESTAMP WITHOUT TIME ZONE, 
	updated_at TIMESTAMP WITHOUT TIME ZONE, 
	PRIMARY KEY (order_id)
);

CREATE TABLE shipping_ledger (
	id SERIAL NOT NULL, 
	order_id VARCHAR, 
	status VARCHAR, 
	created_at TIMESTAMP WITHOUT TIME ZONE, 
	PRIMARY KEY (id)
);

CREATE TABLE shipping_outbox (
	id SERIAL NOT NULL, 
	topic VARCHAR NOT NULL, 
	key VARCHAR NOT NULL, 
	payload VARCHAR NOT NULL, 
	trace_context VARCHAR, 
	created_at TIMESTAMP WITHOUT TIME ZONE, 
	PRIMARY KEY (id)
);

