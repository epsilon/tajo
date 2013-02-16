/*
 * Copyright 2012 Database Lab., Korea Univ.
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

/**
 * 
 */
package tajo.catalog.json;

import com.google.gson.*;
import tajo.catalog.TableMeta;

import java.lang.reflect.Type;

/**
 * @author jihoon
 *
 */
public class TableMetaAdapter implements JsonSerializer<TableMeta>, JsonDeserializer<TableMeta> {

	@Override
	public TableMeta deserialize(JsonElement json, Type typeOfT,
			JsonDeserializationContext context) throws JsonParseException {
		JsonObject jsonObject = json.getAsJsonObject();
		String className = jsonObject.get("classname").getAsJsonPrimitive().getAsString();
		
		Class clazz = null;
		try {
			clazz = Class.forName(className);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new JsonParseException(e);
		}
		return context.deserialize(jsonObject.get("property"), clazz);
	}

	@Override
	public JsonElement serialize(TableMeta src, Type typeOfSrc,
			JsonSerializationContext context) {
		src.mergeProtoToLocal();
		JsonObject jsonObj = new JsonObject();
		String className = src.getClass().getCanonicalName();
		jsonObj.addProperty("classname", className);
		JsonElement jsonElem = context.serialize(src);
		jsonObj.add("property", jsonElem);
		return jsonObj;
	}

}
