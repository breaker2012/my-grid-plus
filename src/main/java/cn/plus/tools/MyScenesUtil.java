package cn.plus.tools;

import cn.plus.model.tmp.MScenesCache;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

public class MyScenesUtil {

    private static Gson gson = new GsonBuilder()
            .enableComplexMapKeySerialization()
            .setDateFormat("yyyy-MM-dd HH:mm:ss")
            .create();

    public static MScenesCache getScenesCache(final String code)
    {
        return gson.fromJson(code, new TypeToken<MScenesCache>() {}.getType());
    }


}
