import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:spark_overview/features/main/bloc/main_cubit.dart';
import 'package:spark_overview/features/main/repository/main_repository.dart';
import 'package:spark_overview/features/main/view/main_controller.dart';

class AppController extends StatelessWidget {
  const AppController({super.key});

  @override
  Widget build(BuildContext context) {
    return MultiRepositoryProvider(
      providers: [RepositoryProvider(create: (context) => MainRepository())],
      child: MultiBlocProvider(
        providers: [
          BlocProvider(
              create: (context) => MainCubit(context.read<MainRepository>()))
        ],
        child: MaterialApp(
          theme: ThemeData(useMaterial3: true),
          routes: {'/': (context) => const MainController()},
        ),
      ),
    );
  }
}
