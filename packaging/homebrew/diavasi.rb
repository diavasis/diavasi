# Homebrew Formula for diavasi
#
# Copy to your tap as Formula/diavasi.rb and fill sha256 from Release SHA256SUMS.
class Diavasi < Formula
  desc "Durable consumer groups over existing databases"
  homepage "https://github.com/diavasis/diavasi"
  version "0.13.0"
  license "Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/diavasis/diavasi/releases/download/v#{version}/diavasi-v#{version}-macos-aarch64.tar.gz"
      # sha256 "REPLACE_AFTER_RELEASE"
    end
    on_intel do
      url "https://github.com/diavasis/diavasi/releases/download/v#{version}/diavasi-v#{version}-macos-x86_64.tar.gz"
      # sha256 "REPLACE_AFTER_RELEASE"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/diavasis/diavasi/releases/download/v#{version}/diavasi-v#{version}-linux-aarch64.tar.gz"
      # sha256 "REPLACE_AFTER_RELEASE"
    end
    on_intel do
      url "https://github.com/diavasis/diavasi/releases/download/v#{version}/diavasi-v#{version}-linux-x86_64.tar.gz"
      # sha256 "REPLACE_AFTER_RELEASE"
    end
  end

  def install
    bin.install "diavasi"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/diavasi --version")
  end
end
