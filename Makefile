CXX      = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O3 -march=native -flto=8 -fopenmp -Iinclude
LDFLAGS  = -fopenmp -flto=8 -lcurl -lssh2 -lpthread -lssl -lcrypto -lz

SRCS = src/main.cpp \
       src/sftp.cpp \
       src/csv.cpp \
       src/api.cpp \
       src/metrics.cpp \
       src/logger.cpp

OBJS = $(SRCS:.cpp=.o)
TARGET = trabajoparalelo

.PHONY: all clean run

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(OBJS) -o $@ $(LDFLAGS)

src/%.o: src/%.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)

run: $(TARGET)
	./$(TARGET)
