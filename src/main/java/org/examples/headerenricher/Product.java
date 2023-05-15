package org.examples.headerenricher;

import java.math.BigDecimal;

record Product(String name, ProductType type, BigDecimal price) {
}
