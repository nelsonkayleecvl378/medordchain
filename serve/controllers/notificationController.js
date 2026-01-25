const asyncHandler = require("../middleware/asyncHandler");
const notificationService = require("../services/notificationService");

const listNotifications = asyncHandler((req, res) => {
  const result = notificationService.getUserNotifications(req.user.id);
  res.json(result);
});

const markAsRead = asyncHandler((req, res) => {
  const notification = notificationService.markAsRead(
    req.params.notificationId,
    req.user.id
  );
  res.json({ notification });
});

const markAllAsRead = asyncHandler((req, res) => {
  const result = notificationService.markAllAsRead(req.user.id);
  res.json(result);
});

module.exports = {
  listNotifications,
  markAsRead,
  markAllAsRead,
};
