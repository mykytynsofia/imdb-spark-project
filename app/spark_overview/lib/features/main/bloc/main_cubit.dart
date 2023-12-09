import 'package:equatable/equatable.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:spark_overview/features/main/repository/main_repository.dart';

part 'main_state.dart';

class MainCubit extends Cubit<MainState> {
  MainCubit(this.repository)
      : super(const MainState(status: Status.initial, data: []));

  final MainRepository repository;

  Future<void> getTitleRatingsData(int option) async {
    emit(state.copyWith(status: Status.loading));

    List<dynamic> results = await repository.getTitleRatingsData(option);

    emit(state.copyWith(data: results, status: Status.success));
  }
}
