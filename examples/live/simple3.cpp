// Simple3.cpp: Live CME data MBP1 messages. Track 10-second window volume and price.
// Output: Symbol Timestamp, (bid_size, bid_price, ask_price, ask_size) Last_price, Last_size, Buy_volume, Sell_volume, Imbalance_10s, AvgPrice_10s, AvgSize_10s
// Imbalance_10s: (buy_vol - sell_vol) / (buy_vol + sell_vol + 1)
// AvgPrice_10s: Volume-weighted average price
// AvgSize_10s: Average trade size
// Output format:
// Symbol Timestamp, (bid_size, bid_price, ask_price, ask_size) Last_price, Last_size, Buy_volume, Sell_volume, Imbalance_10s, AvgPrice_10s, AvgSize_10s
// Example:
// ESH6 12:00:00, (100, 100.00, 100.00, 100) 100.00, 100, 1000, 1000, 0.0000, 100.00, 100.00
// ESH6 12:00:01, (100, 100.00, 100.00, 100) 100.00, 100, 1000, 1000, 0.0000, 100.00, 100.00
// ESH6 12:00:02, (100, 100.00, 100.00, 100) 100.00, 100, 1000, 1000, 0.0000, 100.00, 100.00

#include <atomic>
#include <chrono>
#include <csignal>  // sig_atomic_t
#include <cstdint>
#include <databento/constants.hpp>
#include <databento/dbn.hpp>
#include <databento/datetime.hpp>
#include <databento/enums.hpp>
#include <databento/live.hpp>
#include <databento/live_threaded.hpp>
#include <databento/log.hpp>
#include <databento/pretty.hpp>
#include <databento/record.hpp>
#include <databento/symbol_map.hpp>
#include <databento/with_ts_out.hpp>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <ctime>
#include <deque>

namespace db = databento;

static std::sig_atomic_t volatile gSignal;

// Trade entry for volume tracking in time windows
struct TradeEntry {
  std::chrono::system_clock::time_point timestamp;
  std::int64_t price;
  std::uint32_t size;
  db::Side side;
};

// Per-instrument market data state
struct InstrumentState {
  std::int64_t bid_px = 0;
  std::uint32_t bid_sz = 0;
  std::int64_t ask_px = 0;
  std::uint32_t ask_sz = 0;
  std::int64_t last_price = 0;
  std::uint32_t last_size = 0;
  db::Side last_side = db::Side::None;
  std::uint64_t buy_volume = 0;
  std::uint64_t sell_volume = 0;
  std::deque<TradeEntry> recent_trades;  // For 10-second window tracking
};

int main() {
  db::PitSymbolMap symbol_mappings;
  auto log_receiver = std::make_unique<db::ConsoleLogReceiver>(db::LogLevel::Debug);

  auto client = db::LiveThreaded::Builder()
                    .SetLogReceiver(log_receiver.get())
                    .SetSendTsOut(true)
                    .SetKeyFromEnv()
                    .SetDataset(db::Dataset::GlbxMdp3)
                    .BuildThreaded();

  // Set up signal handler for Ctrl+C
  std::signal(SIGINT, [](int signal) { gSignal = signal; });

  std::vector<std::string> symbols{"ESH6", "ESH6 C6950", "ESH6 P6950"};
  client.Subscribe(symbols, db::Schema::Definition, db::SType::RawSymbol);
  client.Subscribe(symbols, db::Schema::Mbp1, db::SType::RawSymbol);
  client.Subscribe(symbols, db::Schema::Trades, db::SType::RawSymbol);

  // Per-instrument state (protected by mutex)
  std::mutex state_mutex;
  std::unordered_map<std::uint32_t, InstrumentState> instrument_states;

  auto metadata_callback = [](db::Metadata&& metadata) {
    std::cout << metadata << '\n';
  };
  auto record_callback = [&symbol_mappings, &instrument_states, &state_mutex](const db::Record& rec) {
    using db::RType;
    switch (rec.RType()) {
      case RType::Mbp1: {
        // Handle MBP-1 messages - update inside market
        // MBP-1 messages always contain the current best bid/ask in levels[0]
        auto mbp1 = rec.Get<db::WithTsOut<db::Mbp1Msg>>();
        auto instrument_id = mbp1.rec.hd.instrument_id;
        
        std::lock_guard<std::mutex> lock(state_mutex);
        auto& state = instrument_states[instrument_id];
        const auto& level = mbp1.rec.levels[0];
        state.bid_px = level.bid_px;
        state.bid_sz = level.bid_sz;
        state.ask_px = level.ask_px;
        state.ask_sz = level.ask_sz;
        break;
      }
      case RType::Mbp0: {
        // Handle Trade messages (Mbp0 = TradeMsg)
        auto trade = rec.Get<db::WithTsOut<db::TradeMsg>>();
        auto instrument_id = trade.rec.hd.instrument_id;
        
        if (trade.rec.action == db::Action::Trade) {
          std::lock_guard<std::mutex> lock(state_mutex);
          auto& state = instrument_states[instrument_id];
          state.last_price = trade.rec.price;
          state.last_size = trade.rec.size;
          state.last_side = trade.rec.side;
          
          // Aggregate total volume
          if (trade.rec.side == db::Side::Bid) {
            state.buy_volume += trade.rec.size;
          } else if (trade.rec.side == db::Side::Ask) {
            state.sell_volume += trade.rec.size;
          }
          
          // Add to recent trades for 10-second window tracking
          auto now = std::chrono::system_clock::now();
          state.recent_trades.push_back({now, trade.rec.price, trade.rec.size, trade.rec.side});
          
          // Clean up trades older than 10 seconds (keep some buffer, clean up 15 seconds old)
          const auto cleanup_threshold = now - std::chrono::seconds{15};
          while (!state.recent_trades.empty() && 
                 state.recent_trades.front().timestamp < cleanup_threshold) {
            state.recent_trades.pop_front();
          }
        }
        break;
      }
      case RType::InstrumentDef: {
        std::cout << "Received definition: " << rec.Get<db::InstrumentDefMsg>() << '\n';
        break;
      }
      case RType::SymbolMapping: {
        auto mapping = rec.Get<db::SymbolMappingMsg>();
        symbol_mappings.OnSymbolMapping(mapping);
        break;
      }
      case RType::System: {
        const auto& system_msg = rec.Get<db::SystemMsg>();
        if (!system_msg.IsHeartbeat()) {
          std::cout << "Received system msg: " << system_msg.Msg() << '\n';
        }
        break;
      }
      case RType::Error: {
        std::cerr << "Received error from gateway: " << rec.Get<db::ErrorMsg>().Err()
                  << '\n';
        break;
      }
      default: {
        // Silently ignore unknown records
        break;
      }
    }
    return db::KeepGoing::Continue;
  };
  
  client.Start(metadata_callback, record_callback);
  
  // Print thread - outputs state every second
  std::thread print_thread([&symbol_mappings, &instrument_states, &state_mutex]() {
    while (::gSignal == 0) {
      std::this_thread::sleep_for(std::chrono::seconds{1});
      
      auto now = std::chrono::system_clock::now();
      
      // Convert to Chicago time (UTC-6 for CST, UTC-5 for CDT)
      // For simplicity, we'll use UTC-6 (CST) - can be enhanced to detect DST
      const int chicago_offset_hours = -6;
      auto chicago_time = now + std::chrono::hours(chicago_offset_hours);
      auto time_t_chicago = std::chrono::system_clock::to_time_t(chicago_time);
      
      std::tm tm_chicago = {};
#ifdef _WIN32
      ::gmtime_s(&tm_chicago, &time_t_chicago);
#else
      ::gmtime_r(&time_t_chicago, &tm_chicago);
#endif
      
      // Format as HH:MM:SS
      std::ostringstream time_ss;
      time_ss << std::setfill('0') << std::setw(2) << tm_chicago.tm_hour << ':'
              << std::setw(2) << tm_chicago.tm_min << ':'
              << std::setw(2) << tm_chicago.tm_sec;
      std::string time_str = time_ss.str();
      
      std::lock_guard<std::mutex> lock(state_mutex);
      
      const auto window_start = now - std::chrono::seconds{10};
      
      // Print state for each instrument with aligned columns
      for (auto& [instrument_id, state] : instrument_states) {
        std::string symbol_name;
        try {
          symbol_name = symbol_mappings[instrument_id];
        } catch (...) {
          symbol_name = "Unknown(" + std::to_string(instrument_id) + ")";
        }
        
        // Calculate 10-second window metrics
        std::uint64_t buy_volume_10s = 0;
        std::uint64_t sell_volume_10s = 0;
        std::uint64_t total_size_10s = 0;
        std::int64_t price_size_sum_10s = 0;  // For volume-weighted average price
        std::uint32_t trade_count_10s = 0;
        
        for (const auto& trade : state.recent_trades) {
          if (trade.timestamp >= window_start) {
            trade_count_10s++;
            total_size_10s += trade.size;
            price_size_sum_10s += trade.price * static_cast<std::int64_t>(trade.size);
            
            if (trade.side == db::Side::Bid) {
              buy_volume_10s += trade.size;
            } else if (trade.side == db::Side::Ask) {
              sell_volume_10s += trade.size;
            }
          }
        }
        
        // Calculate buy/sell imbalance for 10-second window
        // Formula: (buy_vol - sell_vol) / (buy_vol + sell_vol + 1)
        double imbalance_10s = 0.0;
        std::uint64_t total_volume_10s = buy_volume_10s + sell_volume_10s;
        if (total_volume_10s > 0) {
          imbalance_10s = (static_cast<double>(buy_volume_10s) - static_cast<double>(sell_volume_10s)) /
                          (static_cast<double>(total_volume_10s) + 1.0);
        }
        
        // Calculate windowed averages
        double avg_price_10s = 0.0;
        double avg_size_10s = 0.0;
        if (total_size_10s > 0) {
          // Volume-weighted average price
          avg_price_10s = static_cast<double>(price_size_sum_10s) / static_cast<double>(total_size_10s) / 1e9;
        }
        if (trade_count_10s > 0) {
          // Average trade size
          avg_size_10s = static_cast<double>(total_size_10s) / static_cast<double>(trade_count_10s);
        }
        
        // Convert prices from fixed-point to double (divide by 1e9)
        double bid_price = static_cast<double>(state.bid_px) / 1e9;
        double ask_price = static_cast<double>(state.ask_px) / 1e9;
        double last_price = static_cast<double>(state.last_price) / 1e9;
        
        // Format: Symbol Timestamp, (bid_size, bid_price, ask_price, ask_size) Last_price, Last_size, 
        //         Buy_vol, Sell_vol, Imbalance_10s, AvgPrice_10s, AvgSize_10s
        std::cout << std::left << std::setw(12) << symbol_name
                  << std::right << std::setw(9) << time_str << ", ("
                  << std::setw(4) << state.bid_sz << ", "
                  << std::fixed << std::setprecision(2) << std::setw(10) << bid_price
                  << ", " << std::setw(10) << ask_price
                  << ", " << std::setw(4) << state.ask_sz << ") "
                  << std::setw(10) << last_price << ", "
                  << std::setw(4) << state.last_size << ", "
                  << std::setw(6) << state.buy_volume << ", "
                  << std::setw(6) << state.sell_volume << ", "
                  << std::setprecision(4) << std::setw(8) << imbalance_10s << ", "
                  << std::setprecision(2) << std::setw(10) << avg_price_10s << ", "
                  << std::setprecision(1) << std::setw(8) << avg_size_10s << '\n';
      }
    }
  });
  
  while (::gSignal == 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds{100});
  }
  std::cout << "Exiting..." << std::endl;
  print_thread.join();
  std::cout << "Print thread joined" << std::endl;
  return 0;
}
