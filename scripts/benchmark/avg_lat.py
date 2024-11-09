import numpy as np
import sys
# import matplotlib.pyplot as plt

def calculate_cdf(numbers):
    sorted_numbers = np.sort(numbers)
    n = len(numbers)
    cdf = np.arange(1, n + 1) / n
    return sorted_numbers, cdf

def calculate_p(numbers, p):
    return np.percentile(numbers, p)

def calculate_std_dev(numbers):
    return np.std(numbers)

def main():
    file_names = sys.argv[1:]
    numbers = []

    for file_name in file_names:
        try:
            with open(file_name, 'r') as file:
                number = [float(line.strip()) for line in file.readlines()]
                numbers += number[100:]
        except FileNotFoundError:
            sys.exit("File not found. Please make sure the file exists: " + file_name)
        except ValueError:
            sys.exit("Error: The file contains non-numeric data: " + file_name)

    average = np.mean(numbers)
    std_dev = calculate_std_dev(numbers)
    p99 = calculate_p(numbers, 99)
    p50 = calculate_p(numbers, 50)
    sorted_numbers, cdf = calculate_cdf(numbers)
    print("Average (us), Standard Deviation (us), p50 (us), p99 (us)")
    print(f'{average/1000:<12.3f}, {std_dev/1000:<23.3f}, {p50/1000:<8.3f}, {p99/1000:<8.3f}')

if __name__ == "__main__":
    main()