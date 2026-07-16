CXX=g++
CXXFLAGS=-std=c++17 -O2 -Wall -Wextra -pthread
TARGET_DIR=target
SRC=src/client.cpp src/common.cpp
TARGET=$(TARGET_DIR)/client

all: $(TARGET)
	cp ../server-hostnames.txt $(TARGET_DIR)/server-hostnames.txt

$(TARGET): $(SRC)
	mkdir -p $(TARGET_DIR)
	$(CXX) $(CXXFLAGS) $(SRC) -o $(TARGET)

clean:
	rm -rf $(TARGET_DIR)
