package com.github.knaufk.flinkjunit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

public class TestUtils {

  static String callWebUiOverview(final int port) throws IOException {
    URL url = new URL("http://localhost:" + port + "/overview");

    StringBuilder sb = new StringBuilder();

    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"))) {
      for (String line; (line = reader.readLine()) != null; ) {
        sb.append(line);
      }
    }

    return sb.toString();
  }
}
