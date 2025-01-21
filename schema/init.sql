CREATE TABLE alembic_version (
	version_num VARCHAR(32) NOT NULL, 
	CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);
CREATE TABLE contact_statuses (
	id INTEGER NOT NULL, 
	name VARCHAR NOT NULL, 
	description VARCHAR, 
	PRIMARY KEY (id), 
	UNIQUE (name)
);
CREATE INDEX idx_contact_statuses_name ON contact_statuses (name);
CREATE TABLE forecast_categories (
	id INTEGER NOT NULL, 
	name VARCHAR NOT NULL, 
	description VARCHAR, 
	PRIMARY KEY (id), 
	UNIQUE (name)
);
CREATE INDEX idx_forecast_categories_name ON forecast_categories (name);
CREATE TABLE industries (
	id INTEGER NOT NULL, 
	name VARCHAR NOT NULL, 
	PRIMARY KEY (id), 
	UNIQUE (name)
);
CREATE INDEX idx_industries_name ON industries (name);
CREATE TABLE products (
	id INTEGER NOT NULL, 
	name VARCHAR NOT NULL, 
	description VARCHAR, 
	PRIMARY KEY (id), 
	UNIQUE (name)
);
CREATE INDEX idx_products_name ON products (name);
CREATE TABLE stages (
	id INTEGER NOT NULL, 
	name VARCHAR NOT NULL, 
	description VARCHAR, 
	PRIMARY KEY (id), 
	UNIQUE (name)
);
CREATE INDEX idx_stages_name ON stages (name);
CREATE TABLE companies (
	id VARCHAR NOT NULL, 
	industry_id INTEGER NOT NULL, 
	name VARCHAR NOT NULL, 
	domain VARCHAR NOT NULL, 
	size VARCHAR NOT NULL, 
	country VARCHAR NOT NULL, 
	created_date DATETIME NOT NULL, 
	is_customer BOOLEAN NOT NULL, 
	annual_revenue FLOAT NOT NULL, 
	PRIMARY KEY (id), 
	FOREIGN KEY(industry_id) REFERENCES industries (id), 
	UNIQUE (domain)
);
CREATE INDEX idx_companies_domain ON companies (domain);
CREATE INDEX idx_companies_industry_id ON companies (industry_id);
CREATE TABLE contacts (
	id VARCHAR NOT NULL, 
	status_id INTEGER NOT NULL, 
	company_id VARCHAR NOT NULL, 
	email VARCHAR NOT NULL, 
	first_name VARCHAR NOT NULL, 
	last_name VARCHAR NOT NULL, 
	title VARCHAR NOT NULL, 
	phone VARCHAR, 
	created_date DATETIME NOT NULL, 
	last_modified DATETIME NOT NULL, 
	PRIMARY KEY (id), 
	FOREIGN KEY(company_id) REFERENCES companies (id), 
	FOREIGN KEY(status_id) REFERENCES contact_statuses (id), 
	UNIQUE (email)
);
CREATE INDEX idx_contacts_company_id ON contacts (company_id);
CREATE INDEX idx_contacts_email ON contacts (email);
CREATE INDEX idx_contacts_status_id ON contacts (status_id);
CREATE TABLE opportunities (
	id VARCHAR NOT NULL, 
	name VARCHAR NOT NULL, 
	contact_id VARCHAR NOT NULL, 
	company_id VARCHAR NOT NULL, 
	stage_id INTEGER NOT NULL, 
	forecast_category_id INTEGER NOT NULL, 
	product_id INTEGER NOT NULL, 
	amount FLOAT NOT NULL, 
	probability INTEGER NOT NULL, 
	created_date DATETIME NOT NULL, 
	close_date DATETIME NOT NULL, 
	is_closed BOOLEAN NOT NULL, 
	PRIMARY KEY (id), 
	FOREIGN KEY(company_id) REFERENCES companies (id), 
	FOREIGN KEY(contact_id) REFERENCES contacts (id), 
	FOREIGN KEY(forecast_category_id) REFERENCES forecast_categories (id), 
	FOREIGN KEY(product_id) REFERENCES products (id), 
	FOREIGN KEY(stage_id) REFERENCES stages (id)
);
CREATE INDEX idx_opportunities_company_id ON opportunities (company_id);
CREATE INDEX idx_opportunities_contact_id ON opportunities (contact_id);
CREATE INDEX idx_opportunities_forecast_category_id ON opportunities (forecast_category_id);
CREATE INDEX idx_opportunities_product_id ON opportunities (product_id);
CREATE INDEX idx_opportunities_stage_id ON opportunities (stage_id);
CREATE TABLE activities (
	contact_id VARCHAR NOT NULL, 
	id VARCHAR NOT NULL, 
	type VARCHAR NOT NULL, 
	subject VARCHAR NOT NULL, 
	timestamp DATETIME NOT NULL, 
	duration_minutes INTEGER NOT NULL, 
	outcome VARCHAR NOT NULL, 
	opportunity_id VARCHAR, 
	notes VARCHAR, 
	PRIMARY KEY (id), 
	FOREIGN KEY(contact_id) REFERENCES contacts (id), 
	FOREIGN KEY(opportunity_id) REFERENCES opportunities (id)
);
CREATE INDEX idx_activities_contact_id ON activities (contact_id);
CREATE INDEX idx_activities_opportunity_id ON activities (opportunity_id);
CREATE INDEX idx_activities_timestamp ON activities (timestamp);
