CREATE TABLE interest_rate(pk_rate_id SERIAL PRIMARY KEY,
                            release_date DATE NOT NULL,
                            country VARCHAR(25) NOT NULL,
                            currency CHAR(3) NOT NULL,
                            actual_rate NUMERIC(4,2) NOT NULL)