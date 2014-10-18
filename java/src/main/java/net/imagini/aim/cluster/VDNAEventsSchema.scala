package net.imagini.aim.cluster

import java.util.LinkedHashMap

import scala.collection.JavaConverters.mapAsJavaMapConverter

import net.imagini.aim.types.AimSchema
import net.imagini.aim.types.Aim
import net.imagini.aim.types.AimType

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