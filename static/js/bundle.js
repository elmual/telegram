const swiper = new Swiper(".swiper", {
  effect: "coverflow",
  grabCursor: true,
  centeredSlides: true,
  slidesPerView: 5, // ✅ indi ekranda 5 slayd görsənir
  spaceBetween: 20, // ✅ slaydlar arasında məsafə
  loop: true,
  coverflowEffect: {
    rotate: 30,
    stretch: 0,
    depth: 100,
    modifier: 1,
    slideShadows: true,
  },
  autoplay: {
    delay: 2000,
    disableOnInteraction: false,
    pauseOnMouseEnter: true,
  },
  pagination: {
    el: ".swiper-pagination",
    clickable: true,
  },
  navigation: {
    nextEl: ".swiper-button-next",
    prevEl: ".swiper-button-prev",
  },
  breakpoints: {
    0: { slidesPerView: 1 },
    768: { slidesPerView: 3 },
    1200: { slidesPerView: 3 }, // ✅ böyük ekranda 5
  },
});

AOS.init({
    duration: 1000, // animasiyanın müddəti (ms)
    once: true      // yalnız 1 dəfə animasiya olsun
  });
