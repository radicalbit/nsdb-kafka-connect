/*
 * Copyright 2019 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._
import Keys._

object Dependencies {

  object kafka {
    private lazy val version   = "1.1.1"
    private lazy val namespace = "org.apache.kafka"
    lazy val connect           = namespace % "connect-api" % version excludeAll ExclusionRule(organization = "javax.ws.rs")
  }

  object kcql {
    private lazy val version   = "2.8"
    private lazy val namespace = "com.datamountaineer"
    lazy val kcql              = namespace % "kcql" % version
  }

  object nsdb {
    lazy val namespace = "io.radicalbit.nsdb"
    lazy val  scalaAPI = Def.setting { namespace %% "nsdb-scala-api" % (ThisBuild / version).value}
  }

  object scalatest {
    lazy val version   = "3.0.7"
    lazy val namespace = "org.scalatest"
    lazy val core      = namespace %% "scalatest" % version
  }

  lazy val libraries = Seq(
    kafka.connect % Provided,
    kcql.kcql,
    scalatest.core % Test
  )
}
