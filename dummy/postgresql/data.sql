INSERT INTO person
(person_type, identification_type, identification_number, first_name, last_name, birth_date, nationality)
VALUES
('INDIVIDUAL', 'DNI', '30123456', 'Juan', 'Pérez', '1985-04-12', 'AR'),
('INDIVIDUAL', 'DNI', '28987654', 'María', 'Gómez', '1990-09-21', 'AR'),
('INDIVIDUAL', 'DNI', '33456789', 'Carlos', 'Rodríguez', '1978-01-05', 'UY'),
('INDIVIDUAL', 'DNI', '31222333', 'Laura', 'Fernández', '1988-07-18', 'AR'),
('INDIVIDUAL', 'DNI', '27666777', 'Diego', 'Martínez', '1982-11-30', 'CL'),
('INDIVIDUAL', 'DNI', '35555111', 'Ana', 'López', '1995-03-09', 'AR'),
('INDIVIDUAL', 'DNI', '29999123', 'Federico', 'Ruiz', '1986-06-25', 'AR'),
('INDIVIDUAL', 'DNI', '32111888', 'Sofía', 'Morales', '1992-12-14', 'PE'),
('INDIVIDUAL', 'DNI', '28777444', 'Pablo', 'Suárez', '1980-02-02', 'AR'),
('INDIVIDUAL', 'DNI', '34666222', 'Valentina', 'Castro', '1998-08-27', 'AR');


INSERT INTO customer
(person_id, customer_type, status, onboarding_date, risk_score, segment)
VALUES
(1, 'CLIENT', 'ACTIVE', '2018-05-10', 2.10, 'Retail'),
(2, 'CLIENT', 'ACTIVE', '2020-08-15', 1.50, 'Wealth'),
(3, 'CLIENT', 'ACTIVE', '2016-03-22', 3.80, 'Corporate'),
(4, 'CLIENT', 'INACTIVE', '2019-11-01', 2.90, 'Retail'),
(5, 'PROSPECT', 'ACTIVE', NULL, 4.20, 'Retail'),
(6, 'CLIENT', 'ACTIVE', '2022-02-18', 1.90, 'Retail'),
(7, 'PROSPECT', 'ACTIVE', NULL, 3.10, 'Wealth'),
(8, 'CLIENT', 'ACTIVE', '2021-07-30', 2.40, 'Retail'),
(9, 'CLIENT', 'BLOCKED', '2017-01-12', 4.90, 'Retail'),
(10,'PROSPECT', 'ACTIVE', NULL, 2.70, 'Retail');


INSERT INTO contact_info
(person_id, email, phone, address, city, country, is_primary)
VALUES
(1, 'juan.perez@mail.com', '+54-11-4444-1111', 'Av. Corrientes 123', 'Buenos Aires', 'AR', TRUE),
(2, 'maria.gomez@mail.com', '+54-11-4444-2222', 'Calle Florida 456', 'Buenos Aires', 'AR', TRUE),
(3, 'carlos.rodriguez@mail.com', '+598-2-555-3333', '18 de Julio 789', 'Montevideo', 'UY', TRUE),
(4, 'laura.fernandez@mail.com', '+54-351-555-4444', 'San Martín 100', 'Córdoba', 'AR', TRUE),
(5, 'diego.martinez@mail.com', '+56-2-666-5555', 'Providencia 234', 'Santiago', 'CL', TRUE),
(6, 'ana.lopez@mail.com', '+54-221-555-6666', 'Calle 7 890', 'La Plata', 'AR', TRUE),
(7, 'federico.ruiz@mail.com', '+54-261-555-7777', 'Belgrano 321', 'Mendoza', 'AR', TRUE),
(8, 'sofia.morales@mail.com', '+51-1-555-8888', 'Av. Arequipa 456', 'Lima', 'PE', TRUE),
(9, 'pablo.suarez@mail.com', '+54-341-555-9999', 'Bv. Oroño 654', 'Rosario', 'AR', TRUE),
(10,'valentina.castro@mail.com', '+54-11-555-0000', 'Av. Libertador 987', 'Buenos Aires', 'AR', TRUE);


INSERT INTO financial_product
(product_code, product_name, product_type)
VALUES
('CA', 'Cuenta Ahorro', 'Account'),
('CC', 'Cuenta Corriente', 'Account'),
('TC', 'Tarjeta de Crédito', 'Credit'),
('PF', 'Plazo Fijo', 'Investment');


INSERT INTO customer_product
(customer_id, product_id, start_date, balance, status)
VALUES
(1, 1, '2018-05-10', 150000.50, 'ACTIVE'),
(1, 3, '2019-06-01', 25000.00, 'ACTIVE'),
(2, 1, '2020-08-15', 950000.00, 'ACTIVE'),
(2, 4, '2021-01-10', 2000000.00, 'ACTIVE'),
(3, 2, '2016-03-22', 5000000.00, 'ACTIVE'),
(4, 1, '2019-11-01', 0.00, 'INACTIVE'),
(6, 1, '2022-02-18', 78000.25, 'ACTIVE'),
(8, 3, '2021-07-30', 120000.00, 'ACTIVE'),
(9, 1, '2017-01-12', 3000.00, 'BLOCKED');


INSERT INTO interaction
(customer_id, interaction_type, channel, notes)
VALUES
(5, 'Call', 'Phone', 'Interesado en cuenta ahorro'),
(7, 'Email', 'Web', 'Solicitó información sobre inversiones'),
(10, 'Meeting', 'Branch', 'Primera reunión comercial'),
(5, 'Email', 'Web', 'Se envió propuesta de productos'),
(7, 'Call', 'Phone', 'Pendiente documentación');


