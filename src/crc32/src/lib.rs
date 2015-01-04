#[derive(Copy)]
pub struct Crc32 {
    table: [u32; 256],
    value: u32
}

static CRC32_INITIAL:u32 = 0xedb88320;

impl Crc32 {

    pub fn new() -> Crc32 {
        let mut c = Crc32 { table: [0; 256], value: 0xffffffff };

        for i in range(0u, 256) {
            let mut v = i as u32;
            for _ in range(0i, 8) {
                v = if v & 1 != 0 {
                    CRC32_INITIAL ^ (v >> 1)
                } else {
                    v >> 1
                }
            }
            c.table[i] = v;
        }

        c
    }

    pub fn start(&mut self) {
        self.value = 0xffffffff;
    }

    pub fn update(&mut self, buf: &[u8]) {
        for &i in buf.iter() {
            self.value = self.table[((self.value ^ (i as u32)) & 0xFF) as uint] ^ (self.value >> 8);
        }
    }

    pub fn finalize(&mut self) -> u32 {
        self.value ^ 0xffffffffu32
    }

    pub fn crc(&mut self, buf: &[u8]) -> u32 {
        self.start();
        self.update(buf);
        self.finalize()
    }
}

#[test]
fn test_crc32() {
    let mut buf = [0; 1024 * 1024];
    let mut crc = Crc32::new();

    for arg in os::args().iter().skip(1) {
        let path = Path::new(arg.as_slice());
        let disp = path.display();

        let mut file = match File::open(&path) {
            Ok(file) => file,
            Err(e) => {
                println!("{}: {}", disp, e.desc);
                continue;
            }
        };

        crc.start();

        while match file.read(buf) {
            Ok(len) => {
                crc.update(buf.slice(0, len));
                len > 0
            },
            Err(_) => false
        } { /* do nothing */ };

        println!("{}: {:X}", disp, crc.finalize());
    }
}
