package ai.economicdatasciences.sia.model

import java.time.Instant

case class Order(
  time: Instant,
  orderId: Long,
  clientId: Long,
  symbol: String,
  amount: Int,
  price: Double,
  buy: Boolean
)
