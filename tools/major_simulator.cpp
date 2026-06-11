#include <algorithm>
#include <atomic>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <numeric>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

struct Team {
    int id = 0;
    int seed = 0;
};

struct Record {
    int wins = 0;
    int losses = 0;
    uint32_t faced = 0;

    int diff() const {
        return wins - losses;
    }
};

struct State {
    std::vector<Record> records;
    uint32_t remaining = 0;
};

struct Input {
    int iterations = 200000;
    int threads = 1;
    bool force_bo3 = false;
    std::vector<Team> teams;
    std::vector<std::vector<double>> win_matrix;
    std::vector<std::pair<int, int>> finished_matches;
};

struct ComboKey {
    uint32_t three_zero = 0;
    uint32_t advanced = 0;
    uint32_t zero_three = 0;

    bool operator<(const ComboKey& other) const {
        if (three_zero != other.three_zero) return three_zero < other.three_zero;
        if (advanced != other.advanced) return advanced < other.advanced;
        return zero_three < other.zero_three;
    }
};

using Counts = std::map<ComboKey, uint64_t>;

static Input read_input(const std::string& path) {
    std::ifstream in(path);
    if (!in) throw std::runtime_error("failed to open input file: " + path);

    Input input;
    int force_bo3 = 0;
    int n = 0;
    in >> input.iterations >> input.threads >> force_bo3 >> n;
    input.force_bo3 = force_bo3 != 0;
    std::string line;
    std::getline(in, line);

    input.teams.reserve(n);
    for (int i = 0; i < n; ++i) {
        Team team;
        in >> team.id >> team.seed;
        input.teams.push_back(team);
    }

    input.win_matrix.assign(n, std::vector<double>(n, 0.0));
    for (int i = 0; i < n; ++i) {
        for (int j = 0; j < n; ++j) {
            in >> input.win_matrix[i][j];
        }
    }

    int finished_count = 0;
    in >> finished_count;
    input.finished_matches.reserve(finished_count);
    for (int i = 0; i < finished_count; ++i) {
        int winner = 0;
        int loser = 0;
        in >> winner >> loser;
        input.finished_matches.emplace_back(winner, loser);
    }
    return input;
}

static int games_played(const Record& record) {
    return record.wins + record.losses;
}

static int buchholz(const State& state, int team_id) {
    int sum = 0;
    uint32_t faced = state.records[team_id].faced;
    for (int opp = 0; opp < 32; ++opp) {
        if (faced & (1u << opp)) {
            sum += state.records[opp].diff();
        }
    }
    return sum;
}

static bool active(const State& state, int team_id) {
    return (state.remaining & (1u << team_id)) != 0;
}

static bool is_bo3(const Input& input, const State& state, int team_id) {
    const auto& record = state.records[team_id];
    return input.force_bo3 || record.wins == 2 || record.losses == 2;
}

static double match_win_probability(const Input& input, const State& state, int a, int b) {
    double p = input.win_matrix[a][b];
    if (is_bo3(input, state, a)) {
        return p * p * (3.0 - 2.0 * p);
    }
    return p;
}

static void apply_result(const Input& input, State& state, int winner, int loser) {
    bool bo3 = is_bo3(input, state, winner);
    state.records[winner].wins += 1;
    state.records[loser].losses += 1;
    state.records[winner].faced |= 1u << loser;
    state.records[loser].faced |= 1u << winner;

    if (bo3) {
        for (int team : {winner, loser}) {
            const auto& record = state.records[team];
            if (record.wins == 3 || record.losses == 3) {
                state.remaining &= ~(1u << team);
            }
        }
    }
}

static int next_round_num(const State& state, int n) {
    if (state.remaining == 0) return 6;
    int min_games = 99;
    for (int i = 0; i < n; ++i) {
        if (active(state, i)) {
            min_games = std::min(min_games, games_played(state.records[i]));
        }
    }
    return min_games + 1;
}

static std::vector<std::pair<int, int>> try_match_teams(const Input& input, const State& state, std::vector<int> teams, int round_num) {
    std::vector<std::pair<int, int>> matches;
    int n = static_cast<int>(teams.size());
    if (n < 2) return matches;

    if (round_num == 1) {
        for (int i = 0; i < n / 2; ++i) {
            matches.emplace_back(teams[i], teams[n - 1 - i]);
        }
        return matches;
    }

    auto not_faced = [&](int a, int b) {
        return (state.records[a].faced & (1u << b)) == 0;
    };

    if (round_num <= 3) {
        std::vector<bool> used(n, false);
        for (int i = 0; i < n; ++i) {
            if (used[i]) continue;
            for (int j = n - 1; j > i; --j) {
                if (used[j]) continue;
                if (not_faced(teams[i], teams[j])) {
                    matches.emplace_back(teams[i], teams[j]);
                    used[i] = true;
                    used[j] = true;
                    break;
                }
            }
        }
        return matches;
    }

    const std::vector<std::vector<std::pair<int, int>>> patterns = {
        {{0,5},{1,4},{2,3}}, {{0,5},{1,3},{2,4}}, {{0,4},{1,5},{2,3}},
        {{0,4},{1,3},{2,5}}, {{0,3},{1,5},{2,4}}, {{0,3},{1,4},{2,5}},
        {{0,5},{1,2},{3,4}}, {{0,4},{1,2},{3,5}}, {{0,2},{1,5},{3,4}},
        {{0,2},{1,4},{3,5}}, {{0,3},{1,2},{4,5}}, {{0,2},{1,3},{4,5}},
        {{0,1},{2,5},{3,4}}, {{0,1},{2,4},{3,5}}, {{0,1},{2,3},{4,5}},
    };

    for (const auto& pattern : patterns) {
        matches.clear();
        std::vector<bool> used(n, false);
        bool valid = true;
        for (auto [i, j] : pattern) {
            if (i >= n || j >= n || used[i] || used[j] || !not_faced(teams[i], teams[j])) {
                valid = false;
                break;
            }
            matches.emplace_back(teams[i], teams[j]);
            used[i] = true;
            used[j] = true;
        }
        if (valid) return matches;
    }

    matches.clear();
    std::vector<bool> used(n, false);
    for (int i = 0; i < n; ++i) {
        if (used[i]) continue;
        for (int j = n - 1; j > i; --j) {
            if (used[j]) continue;
            if (not_faced(teams[i], teams[j])) {
                matches.emplace_back(teams[i], teams[j]);
                used[i] = true;
                used[j] = true;
                break;
            }
        }
    }
    return matches;
}

static std::vector<std::pair<int, int>> round_matches(const Input& input, const State& state, int round_num) {
    int n = static_cast<int>(input.teams.size());
    std::map<int, std::vector<int>, std::greater<int>> groups;
    std::vector<int> eligible;
    for (int i = 0; i < n; ++i) {
        if (active(state, i) && games_played(state.records[i]) == round_num - 1) {
            eligible.push_back(i);
            groups[state.records[i].diff()].push_back(i);
        }
    }

    for (auto& [_, teams] : groups) {
        std::sort(teams.begin(), teams.end(), [&](int a, int b) {
            int ba = buchholz(state, a);
            int bb = buchholz(state, b);
            if (ba != bb) return ba > bb;
            return input.teams[a].seed < input.teams[b].seed;
        });
    }

    if (round_num == 1 && groups.count(0) && groups[0].size() == eligible.size()) {
        auto teams = groups[0];
        std::sort(teams.begin(), teams.end(), [&](int a, int b) {
            return input.teams[a].seed < input.teams[b].seed;
        });
        std::vector<std::pair<int, int>> matches;
        for (int i = 0; i < static_cast<int>(teams.size()) / 2; ++i) {
            matches.emplace_back(teams[i], teams[i + teams.size() / 2]);
        }
        return matches;
    }

    std::vector<std::pair<int, int>> matches;
    for (auto& [_, teams] : groups) {
        auto group_matches = try_match_teams(input, state, teams, round_num);
        matches.insert(matches.end(), group_matches.begin(), group_matches.end());
    }
    return matches;
}

static State initial_state(const Input& input) {
    State state;
    int n = static_cast<int>(input.teams.size());
    state.records.assign(n, Record{});
    state.remaining = n >= 32 ? 0xffffffffu : ((1u << n) - 1u);
    for (auto [winner, loser] : input.finished_matches) {
        apply_result(input, state, winner, loser);
    }
    return state;
}

static Counts simulate_batch(const Input& input, int iterations, uint64_t seed) {
    Counts counts;
    int n = static_cast<int>(input.teams.size());
    std::mt19937_64 rng(seed);
    std::uniform_real_distribution<double> dist(0.0, 1.0);

    for (int iter = 0; iter < iterations; ++iter) {
        State state = initial_state(input);
        int round_num = next_round_num(state, n);
        while (state.remaining != 0 && round_num <= 5) {
            auto matches = round_matches(input, state, round_num);
            for (auto [a, b] : matches) {
                double p = match_win_probability(input, state, a, b);
                if (dist(rng) < p) {
                    apply_result(input, state, a, b);
                } else {
                    apply_result(input, state, b, a);
                }
            }
            round_num += 1;
        }

        ComboKey key;
        for (int i = 0; i < n; ++i) {
            const auto& record = state.records[i];
            if (record.wins == 3) {
                if (record.losses == 0) key.three_zero |= 1u << i;
                else key.advanced |= 1u << i;
            } else if (record.losses == 3 && record.wins == 0) {
                key.zero_three |= 1u << i;
            }
        }
        counts[key] += 1;
    }
    return counts;
}

static void write_output(const Input& input, const Counts& counts, const std::string& path) {
    std::ofstream out(path);
    if (!out) throw std::runtime_error("failed to open output file: " + path);

    uint64_t total = 0;
    for (const auto& [_, count] : counts) total += count;

    std::vector<std::pair<ComboKey, uint64_t>> sorted(counts.begin(), counts.end());
    std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
        return a.second > b.second;
    });

    out << "# major_hw_result_format=2\n";

    for (const auto& [key, count] : sorted) {
        double pct = total ? static_cast<double>(count) / static_cast<double>(total) * 100.0 : 0.0;
        out << "m " << std::hex << key.three_zero << " " << key.advanced << " " << key.zero_three
            << std::dec << ": " << count << "/" << total << " "
            << "(" << std::fixed << std::setprecision(4) << pct << "%)\n";
    }
}

int main(int argc, char** argv) {
    if (argc != 3 && argc != 4) {
        std::cerr << "usage: major_simulator <input.txt> <output.txt> [--dump-first-round]\n";
        return 2;
    }

    try {
        Input input = read_input(argv[1]);
        if (argc == 4 && std::string(argv[3]) == "--dump-first-round") {
            State state = initial_state(input);
            int round_num = next_round_num(state, static_cast<int>(input.teams.size()));
            auto matches = round_matches(input, state, round_num);
            std::cout << "round=" << round_num << "\n";
            for (auto [a, b] : matches) {
                std::cout << a << " " << b << "\n";
            }
            return 0;
        }
        input.threads = std::max(1, input.threads);
        input.threads = std::min(input.threads, std::max(1, input.iterations));

        std::vector<std::thread> threads;
        std::vector<Counts> partial(input.threads);
        int base = input.iterations / input.threads;
        int extra = input.iterations % input.threads;
        uint64_t seed_base = std::random_device{}();

        for (int i = 0; i < input.threads; ++i) {
            int batch = base + (i < extra ? 1 : 0);
            threads.emplace_back([&, i, batch]() {
                partial[i] = simulate_batch(input, batch, seed_base + static_cast<uint64_t>(i) * 1000003ULL);
            });
        }
        for (auto& thread : threads) thread.join();

        Counts merged;
        for (const auto& part : partial) {
            for (const auto& [key, count] : part) {
                merged[key] += count;
            }
        }
        write_output(input, merged, argv[2]);
    } catch (const std::exception& exc) {
        std::cerr << "major_simulator error: " << exc.what() << "\n";
        return 1;
    }
    return 0;
}
