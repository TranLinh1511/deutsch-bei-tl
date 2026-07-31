      import { initializeApp } from "https://www.gstatic.com/firebasejs/11.0.0/firebase-app.js";
      import {
        getAuth,
        createUserWithEmailAndPassword,
        signInWithEmailAndPassword,
        updatePassword,
        sendPasswordResetEmail,
      } from "https://www.gstatic.com/firebasejs/11.0.0/firebase-auth.js";
      import {
        getFirestore,
        doc,
        setDoc,
        getDoc,
      } from "https://www.gstatic.com/firebasejs/11.0.0/firebase-firestore.js";

      const firebaseConfig = {
        apiKey: "AIzaSyAF016vyl1zZ7dSTpmMZkj8BhCQVwmELl0",
        authDomain: "deutschbei-tl.firebaseapp.com",
        projectId: "deutschbei-tl",
        storageBucket: "deutschbei-tl.firebasestorage.app",
        messagingSenderId: "698311590550",
        appId: "1:698311590550:web:fd9006993fb023641138b7",
      };

      const app = initializeApp(firebaseConfig);
      const auth = getAuth(app);
      const db = getFirestore(app);

      // ---- Elements ----
      const usernameInput = document.getElementById("username");
      const passwordInput = document.getElementById("password");
      const loginBtn = document.getElementById("loginBtn");
      const spinner = document.getElementById("spinner");
      const btnText = document.getElementById("btnText");
      const errMsg = document.getElementById("errMsg");
      const pwToggle = document.getElementById("pwToggle");
      const formView = document.getElementById("formView");
      const successView = document.getElementById("successView");
      const successRegView = document.getElementById("successRegView");
      const registerView = document.getElementById("registerView");
      const themeBtn = document.getElementById("themeBtn");
      const statsRow = document.querySelector(".stats-row");
      const regName = document.getElementById("regName");
      const regUsername = document.getElementById("regUsername");
      const regPassword = document.getElementById("regPassword");
      const regConfirm = document.getElementById("regConfirm");
      const registerBtn = document.getElementById("registerBtn");
      const regSpinner = document.getElementById("regSpinner");
      const regBtnText = document.getElementById("regBtnText");
      const regErrMsg = document.getElementById("regErrMsg");
      const regPwToggle = document.getElementById("regPwToggle");
      const regConfirmToggle = document.getElementById("regConfirmToggle");
      const backToLoginLink = document.getElementById("backToLoginLink");
      const forgotView = document.getElementById("forgotView");
      const successResetView = document.getElementById("successResetView");
      const forgotUsername = document.getElementById("forgotUsername");
      const forgotFindBtn = document.getElementById("forgotFindBtn");
      const forgotSpinner = document.getElementById("forgotSpinner");
      const forgotFindText = document.getElementById("forgotFindText");
      const forgotErrMsg = document.getElementById("forgotErrMsg");
      const forgotOkMsg = document.getElementById("forgotOkMsg");
      const forgotStep1 = document.getElementById("forgotStep1");
      const forgotStep2 = document.getElementById("forgotStep2");
      const foundUserBox = document.getElementById("foundUserBox");
      const newPassword = document.getElementById("newPassword");
      const newConfirm = document.getElementById("newConfirm");
      const resetPwBtn = document.getElementById("resetPwBtn");
      const resetSpinner = document.getElementById("resetSpinner");
      const resetBtnText = document.getElementById("resetBtnText");
      const newPwToggle = document.getElementById("newPwToggle");
      const newConfirmToggle = document.getElementById("newConfirmToggle");

      // ---- Theme ----
      let isDark = localStorage.getItem("loginTheme") !== "light";
      function applyTheme() {
        document.body.classList.toggle("light", !isDark);
        themeBtn.innerHTML = isDark
          ? '<i class="fa-solid fa-moon"></i>'
          : '<i class="fa-solid fa-sun"></i>';
      }
      applyTheme();
      themeBtn.addEventListener("click", () => {
        isDark = !isDark;
        localStorage.setItem("loginTheme", isDark ? "dark" : "light");
        applyTheme();
      });

      // ---- View switcher ----
      function showView(view) {
        formView.style.display = "none";
        registerView.style.display = "none";
        forgotView.style.display = "none";
        successView.style.display = "none";
        successRegView.style.display = "none";
        successResetView.style.display = "none";
        statsRow.style.display = ["login", "success"].includes(view)
          ? ""
          : "none";
        if (view === "login") formView.style.display = "";
        if (view === "register") registerView.style.display = "";
        if (view === "forgot") {
          forgotView.style.display = "";
          resetForgotForm();
        }
        if (view === "success") successView.style.display = "block";
        if (view === "successReg") successRegView.style.display = "block";
        if (view === "successReset") successResetView.style.display = "block";
      }

      // ---- Helpers ----
      // Firebase dùng email nên ta tự tạo email giả từ username
      function toFakeEmail(username) {
        return (
          username.trim().toLowerCase().replace(/\s+/g, "_") + "@beitl.app"
        );
      }

      // ---- Password toggles ----
      pwToggle.addEventListener("click", () => {
        const isPass = passwordInput.type === "password";
        passwordInput.type = isPass ? "text" : "password";
        pwToggle.innerHTML = isPass
          ? '<i class="fa-solid fa-eye-slash"></i>'
          : '<i class="fa-regular fa-eye"></i>';
        passwordInput.focus();
      });
      regPwToggle.addEventListener("click", () => {
        const isPass = regPassword.type === "password";
        regPassword.type = isPass ? "text" : "password";
        regPwToggle.innerHTML = isPass
          ? '<i class="fa-solid fa-eye-slash"></i>'
          : '<i class="fa-regular fa-eye"></i>';
        regPassword.focus();
      });
      regConfirmToggle.addEventListener("click", () => {
        const isPass = regConfirm.type === "password";
        regConfirm.type = isPass ? "text" : "password";
        regConfirmToggle.innerHTML = isPass
          ? '<i class="fa-solid fa-eye-slash"></i>'
          : '<i class="fa-regular fa-eye"></i>';
        regConfirm.focus();
      });
      newPwToggle.addEventListener("click", () => {
        const isPass = newPassword.type === "password";
        newPassword.type = isPass ? "text" : "password";
        newPwToggle.innerHTML = isPass
          ? '<i class="fa-solid fa-eye-slash"></i>'
          : '<i class="fa-regular fa-eye"></i>';
        newPassword.focus();
      });
      newConfirmToggle.addEventListener("click", () => {
        const isPass = newConfirm.type === "password";
        newConfirm.type = isPass ? "text" : "password";
        newConfirmToggle.innerHTML = isPass
          ? '<i class="fa-solid fa-eye-slash"></i>'
          : '<i class="fa-regular fa-eye"></i>';
        newConfirm.focus();
      });

      // ---- Switch views ----
      document.querySelector(".new-user a").addEventListener("click", (e) => {
        e.preventDefault();
        clearRegForm();
        showView("register");
        setTimeout(() => regName.focus(), 50);
      });
      document.querySelector(".forgot").addEventListener("click", (e) => {
        e.preventDefault();
        showView("forgot");
        setTimeout(() => forgotUsername.focus(), 50);
      });
      document
        .getElementById("backFromForgot")
        .addEventListener("click", () => {
          showView("login");
          setTimeout(() => usernameInput.focus(), 50);
        });
      backToLoginLink.addEventListener("click", (e) => {
        e.preventDefault();
        showView("login");
        setTimeout(() => usernameInput.focus(), 50);
      });

      // ---- Login ----
      [usernameInput, passwordInput].forEach((el) => {
        el.addEventListener("input", () => {
          el.classList.remove("err");
          errMsg.style.display = "none";
        });
        el.addEventListener("keydown", (e) => {
          if (e.key === "Enter") doLogin();
        });
      });
      loginBtn.addEventListener("click", doLogin);

      async function doLogin() {
        const u = usernameInput.value.trim();
        const p = passwordInput.value;
        errMsg.style.display = "none";
        usernameInput.classList.remove("err", "ok");
        passwordInput.classList.remove("err", "ok");
        if (!u) {
          usernameInput.classList.add("err");
          usernameInput.focus();
          return;
        }
        if (!p) {
          passwordInput.classList.add("err");
          passwordInput.focus();
          return;
        }

        loginBtn.disabled = true;
        spinner.style.display = "block";
        btnText.textContent = "Đang kiểm tra...";

        try {
          const email = toFakeEmail(u);
          const cred = await signInWithEmailAndPassword(auth, email, p);
          // Lấy displayName từ Firestore
          const snap = await getDoc(doc(db, "users", cred.user.uid));
          const displayName = snap.exists() ? snap.data().name : u;

          spinner.style.display = "none";
          btnText.innerHTML = '<i class="fa-solid fa-check"></i> Thành công!';
          usernameInput.classList.add("ok");
          passwordInput.classList.add("ok");

          setTimeout(() => {
            showView("success");
            const remember = document.getElementById("remember").checked;
            const storage = remember ? localStorage : sessionStorage;
            storage.setItem("loggedIn", "1");
            storage.setItem("loggedUser", displayName);
            storage.setItem("loggedUid", cred.user.uid);
            setTimeout(() => {
              window.location.href = "index.html";
            }, 1500);
          }, 600);
        } catch (e) {
          loginBtn.disabled = false;
          spinner.style.display = "none";
          btnText.textContent = "Đăng nhập →";
          usernameInput.classList.add("err");
          passwordInput.classList.add("err");
          errMsg.style.display = "block";
          passwordInput.value = "";
          passwordInput.focus();
        }
      }

      // ---- Register ----
      [regName, regUsername, regPassword, regConfirm].forEach((el) => {
        el.addEventListener("input", () => {
          el.classList.remove("err");
          regErrMsg.style.display = "none";
        });
        el.addEventListener("keydown", (e) => {
          if (e.key === "Enter") doRegister();
        });
      });
      registerBtn.addEventListener("click", doRegister);

      function clearRegForm() {
        [regName, regUsername, regPassword, regConfirm].forEach((el) => {
          el.value = "";
          el.classList.remove("err", "ok");
        });
        regErrMsg.style.display = "none";
        regBtnText.textContent = "Tạo tài khoản →";
        registerBtn.disabled = false;
        regSpinner.style.display = "none";
      }

      function escapeHtml(s) {
        const d = document.createElement("div");
        d.textContent = s;
        return d.innerHTML;
      }

      function showRegErr(msg, ...fields) {
        regErrMsg.innerHTML =
          '<i class="fa-solid fa-triangle-exclamation"></i> ' + escapeHtml(msg);
        regErrMsg.style.display = "block";
        fields.forEach((f) => f.classList.add("err"));
        if (fields[0]) fields[0].focus();
      }

      async function doRegister() {
        const name = regName.value.trim();
        const user = regUsername.value.trim();
        const pass = regPassword.value;
        const conf = regConfirm.value;

        regErrMsg.style.display = "none";
        [regName, regUsername, regPassword, regConfirm].forEach((el) =>
          el.classList.remove("err", "ok"),
        );

        if (!name) return showRegErr("Vui lòng nhập họ tên", regName);
        if (!user)
          return showRegErr("Vui lòng nhập tên đăng nhập", regUsername);
        if (user.length < 3)
          return showRegErr("Tên đăng nhập tối thiểu 3 ký tự", regUsername);
        if (!pass) return showRegErr("Vui lòng nhập mật khẩu", regPassword);
        if (pass.length < 6)
          return showRegErr("Mật khẩu tối thiểu 6 ký tự", regPassword);
        if (pass !== conf)
          return showRegErr("Mật khẩu xác nhận không khớp", regConfirm);

        registerBtn.disabled = true;
        regSpinner.style.display = "block";
        regBtnText.textContent = "Đang tạo tài khoản...";

        try {
          const email = toFakeEmail(user);
          const cred = await createUserWithEmailAndPassword(auth, email, pass);
          // Lưu tên hiển thị vào Firestore
          await setDoc(doc(db, "users", cred.user.uid), {
            name,
            username: user,
            createdAt: Date.now(),
          });

          regSpinner.style.display = "none";
          regBtnText.innerHTML = '<i class="fa-solid fa-check"></i> Thành công!';
          [regName, regUsername, regPassword, regConfirm].forEach((el) =>
            el.classList.add("ok"),
          );

          setTimeout(() => {
            showView("successReg");
            setTimeout(() => {
              usernameInput.value = user;
              passwordInput.value = "";
              showView("login");
              usernameInput.classList.add("ok");
              passwordInput.focus();
            }, 2000);
          }, 600);
        } catch (e) {
          registerBtn.disabled = false;
          regSpinner.style.display = "none";
          regBtnText.textContent = "Tạo tài khoản →";
          if (e.code === "auth/email-already-in-use") {
            showRegErr("Tên đăng nhập đã tồn tại", regUsername);
          } else {
            showRegErr("Lỗi: " + e.message);
          }
        }
      }

      // ---- Forgot password ----
      function resetForgotForm() {
        forgotUsername.value = "";
        forgotUsername.classList.remove("err", "ok");
        forgotErrMsg.style.display = "none";
        forgotOkMsg.style.display = "none";
        forgotStep1.style.display = "";
        forgotStep2.style.display = "none";
        forgotFindBtn.disabled = false;
        forgotSpinner.style.display = "none";
        forgotFindText.textContent = "Tìm tài khoản →";
        newPassword.value = "";
        newPassword.classList.remove("err", "ok");
        newConfirm.value = "";
        newConfirm.classList.remove("err", "ok");
        resetPwBtn.disabled = false;
        resetSpinner.style.display = "none";
        resetBtnText.textContent = "Đặt lại mật khẩu →";
      }

      function showForgotErr(msg, ...fields) {
        forgotErrMsg.innerHTML =
          '<i class="fa-solid fa-triangle-exclamation"></i> ' + escapeHtml(msg);
        forgotErrMsg.style.color = "var(--red)";
        forgotErrMsg.style.background = "rgba(247,129,102,.1)";
        forgotErrMsg.style.border = "1px solid rgba(247,129,102,.35)";
        forgotErrMsg.style.display = "block";
        forgotOkMsg.style.display = "none";
        fields.forEach((f) => f.classList.add("err"));
        if (fields[0]) fields[0].focus();
      }

      forgotUsername.addEventListener("input", () => {
        forgotUsername.classList.remove("err");
        forgotErrMsg.style.display = "none";
      });
      forgotUsername.addEventListener("keydown", (e) => {
        if (e.key === "Enter") doFindUser();
      });
      forgotFindBtn.addEventListener("click", doFindUser);

      let foundUserCred = null;

      async function doFindUser() {
        const u = forgotUsername.value.trim();
        forgotErrMsg.style.display = "none";
        forgotUsername.classList.remove("err", "ok");
        if (!u)
          return showForgotErr("Vui lòng nhập tên đăng nhập", forgotUsername);

        forgotFindBtn.disabled = true;
        forgotSpinner.style.display = "block";
        forgotFindText.textContent = "Đang tìm...";

        try {
          // Gửi reset email qua Firebase
          await sendPasswordResetEmail(auth, toFakeEmail(u));
          forgotSpinner.style.display = "none";
          forgotFindBtn.disabled = false;
          forgotFindText.textContent = "Tìm tài khoản →";
          forgotUsername.classList.add("ok");
          forgotOkMsg.innerHTML =
            '<i class="fa-solid fa-circle-check"></i> Đã gửi email đặt lại mật khẩu!';
          forgotOkMsg.style.display = "block";
          forgotErrMsg.style.display = "none";
        } catch (e) {
          forgotSpinner.style.display = "none";
          forgotFindBtn.disabled = false;
          forgotFindText.textContent = "Tìm tài khoản →";
          showForgotErr("Không tìm thấy tài khoản này", forgotUsername);
        }
      }

      [newPassword, newConfirm].forEach((el) => {
        el.addEventListener("input", () => {
          el.classList.remove("err");
          forgotErrMsg.style.display = "none";
        });
      });
      resetPwBtn.addEventListener("click", () => {});
