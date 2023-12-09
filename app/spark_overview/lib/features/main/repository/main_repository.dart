import 'dart:convert';

import 'package:http/http.dart';

class MainRepository {
  Future<List<dynamic>> getTitleRatingsData(int option) async {
    try {
      List<dynamic> results = [];

      final Response response = await get(Uri.parse(''));

      if (response.statusCode == 200) {
        final Map<String, dynamic> json = jsonDecode(response.body);

        results = json['results'] as List<dynamic>;
      }

      return results;
    } catch (_) {
      rethrow;
    }
  }
}
