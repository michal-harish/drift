package net.imagini.aim.cluster

import net.imagini.aim.AimSchema
import scala.collection.JavaConverters._
import java.util.LinkedHashMap
import net.imagini.aim.AimType
import net.imagini.aim.Aim

object VDNAEventsSchema extends AimSchema(new LinkedHashMap(Map[String, AimType](

  "timestamp" -> Aim.LONG,
  "client_ip" -> Aim.IPV4(Aim.INT),
  "event_type" -> Aim.STRING,
  "action" -> Aim.STRING,
  "user_agent" -> Aim.STRING,
  "country_code" -> Aim.BYTEARRAY(2),
  "region_code" -> Aim.BYTEARRAY(3),
  "post_code" -> Aim.STRING,
  "api_key" -> Aim.STRING,
  "url" -> Aim.STRING,
  "user_uid" -> Aim.UUID(Aim.BYTEARRAY(16)),
  "user_quizzed" -> Aim.BOOL

).asJava)) {}